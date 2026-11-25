import type {
  Dictionary,
  IEventDispatcher,
  ILogger,
  IRequestContext,
  RequestContext,
} from '@xeno-js/core'
import { AppError, Result } from '@xeno-js/core'
import type { Mock } from 'vitest'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('drizzle-orm', () => ({
  sql: {
    raw: vi.fn((query: string) => query),
  },
}))

import { OutboxWorker } from '../background.worker'
import type { DbContext } from '../db.types'

interface OutboxRow {
  id: string
  type: string
  payload: unknown
  ['aggregate_id']: string
  ['retry_count']?: number
}

interface TransactionStub {
  execute: Mock<(...args: unknown[]) => Promise<{ rows: OutboxRow[] } | undefined>>
}

type TransactionCallback = <T>(callback: (tx: TransactionStub) => Promise<T>) => Promise<T>

const createRow = (overrides: Partial<OutboxRow> = {}): OutboxRow => ({
  id: 'message-1',
  type: 'user.created',
  payload: '{"name":"Ada"}',
  ['aggregate_id']: 'aggregate-1',
  ['retry_count']: 0,
  ...overrides,
})

describe('OutboxWorker', () => {
  let transaction: TransactionStub
  let transact: Mock<TransactionCallback>
  let db: DbContext<Dictionary>
  let publish: Mock<IEventDispatcher['publish']>
  let dispatcher: IEventDispatcher
  let logInfo: Mock<ILogger['info']>
  let logError: Mock<ILogger['error']>
  let logger: ILogger
  let runAsync: Mock<IRequestContext<RequestContext>['runAsync']>
  let requestContext: IRequestContext<RequestContext>

  beforeEach(() => {
    vi.useFakeTimers()
    transaction = {
      execute: vi.fn<(...args: unknown[]) => Promise<{ rows: OutboxRow[] } | undefined>>(),
    }
    transact = vi.fn(async <T>(callback: (tx: TransactionStub) => Promise<T>) =>
      callback(transaction),
    )
    db = { transaction: transact } as unknown as DbContext<Dictionary>
    publish = vi.fn<IEventDispatcher['publish']>().mockResolvedValue(Result.ok())
    dispatcher = {
      publish,
      subscribe: vi.fn<IEventDispatcher['subscribe']>(),
    }
    logInfo = vi.fn<ILogger['info']>()
    logError = vi.fn<ILogger['error']>()
    logger = {
      info: logInfo,
      warn: vi.fn<ILogger['warn']>(),
      error: logError,
      debug: vi.fn<ILogger['debug']>(),
    }
    runAsync = vi.fn(async <T>(_context: RequestContext, callback: () => Promise<T>) => callback())
    requestContext = {
      runAsync,
      updateIdentity: vi.fn<IRequestContext<RequestContext>['updateIdentity']>(),
    } as unknown as IRequestContext<RequestContext>
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts once, reports the interval, and schedules subsequent polls', async () => {
    transaction.execute.mockResolvedValue({ rows: [] })
    const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

    worker.start(2500)
    worker.start(1000)
    await vi.waitFor(() => expect(transact).toHaveBeenCalledOnce())

    expect(logInfo).toHaveBeenCalledOnce()
    expect(logInfo).toHaveBeenCalledWith('Outbox Worker started. Polling 1 tables every 2500 ms.')
    await vi.advanceTimersByTimeAsync(2500)
    expect(transact).toHaveBeenCalledTimes(2)

    worker.stop()
    await vi.advanceTimersByTimeAsync(2500)
    expect(transact).toHaveBeenCalledTimes(2)
  })

  it('uses the default interval and stops a worker with no configured tables', async () => {
    const worker = new OutboxWorker(db, dispatcher, logger, [], requestContext)

    worker.start()
    await Promise.resolve()

    expect(logInfo).toHaveBeenCalledWith('Outbox Worker started. Polling 0 tables every 5000 ms.')
    expect(transact).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(1)

    worker.stop()
    await vi.advanceTimersByTimeAsync(5000)
    expect(transact).not.toHaveBeenCalled()
  })

  it('does not schedule another poll when stopped during an active poll', async () => {
    let releasePoll: ((value: { rows: OutboxRow[] }) => void) | undefined
    transaction.execute.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releasePoll = resolve
        }),
    )
    const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

    worker.start(1000)
    await vi.waitFor(() => expect(transaction.execute).toHaveBeenCalledOnce())
    worker.stop()

    if (releasePoll === undefined) throw new Error('Poll query was not started')
    releasePoll({ rows: [] })
    await Promise.resolve()
    await Promise.resolve()

    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(1000)
    expect(transact).toHaveBeenCalledOnce()
  })

  it('allows stopping before the first poll', () => {
    const worker = new OutboxWorker(db, dispatcher, logger, [], requestContext)

    worker.stop()

    expect(vi.getTimerCount()).toBe(0)
    expect(transact).not.toHaveBeenCalled()
  })

  it('dispatches pending messages with parsed payloads and marks them published', async () => {
    transaction.execute
      .mockResolvedValueOnce({ rows: [createRow()] })
      .mockResolvedValueOnce(undefined)
    const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

    worker.start()
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce())
    worker.stop()

    expect(publish).toHaveBeenCalledWith({
      eventType: 'user.created',
      aggregateId: 'aggregate-1',
      payload: { name: 'Ada' },
    })
    expect(runAsync).toHaveBeenCalledOnce()
    expect(transaction.execute).toHaveBeenCalledTimes(2)
    expect(String(transaction.execute.mock.calls[1]?.[0])).toContain("SET status = 'PUBLISHED'")
  })

  it('passes non-string payloads through to the event dispatcher', async () => {
    const payload = { name: 'Ada' }
    transaction.execute
      .mockResolvedValueOnce({ rows: [createRow({ payload })] })
      .mockResolvedValueOnce(undefined)
    const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

    worker.start()
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce())
    worker.stop()

    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ payload }))
  })

  it.each([
    { retryCount: 0, nextRetryCount: 1, status: 'PENDING' },
    { retryCount: 2, nextRetryCount: 3, status: 'FAILED' },
  ])(
    'increments retry count and sets status to $status after dispatch failure',
    async ({ retryCount, nextRetryCount, status }) => {
      publish.mockResolvedValue(
        Result.fail(
          AppError.create({
            message: "Couldn't dispatch",
            code: 'DISPATCH_FAILED',
            status: 500,
            name: 'DispatchError',
            cause: undefined,
          }),
        ),
      )
      transaction.execute
        .mockResolvedValueOnce({ rows: [createRow({ retry_count: retryCount })] })
        .mockResolvedValueOnce(undefined)
      const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

      worker.start()
      await vi.waitFor(() => expect(logError).toHaveBeenCalledOnce())
      worker.stop()

      expect(String(transaction.execute.mock.calls[1]?.[0])).toContain(`SET status = '${status}'`)
      expect(String(transaction.execute.mock.calls[1]?.[0])).toContain(
        `retry_count = ${nextRetryCount}`,
      )
      expect(String(transaction.execute.mock.calls[1]?.[0])).toContain(
        "last_error = 'Couldn''t dispatch'",
      )
      expect(logError).toHaveBeenCalledWith(
        `Outbox routing failed for message-1 (Attempt ${nextRetryCount}):`,
        expect.objectContaining({ message: "Couldn't dispatch" }),
      )
    },
  )

  it('defaults a missing retry count to zero', async () => {
    publish.mockResolvedValue(
      Result.fail(
        AppError.create({
          message: 'temporary failure',
          code: 'DISPATCH_FAILED',
          status: 500,
          name: 'DispatchError',
          cause: undefined,
        }),
      ),
    )
    transaction.execute
      .mockResolvedValueOnce({ rows: [createRow({ ['retry_count']: undefined })] })
      .mockResolvedValueOnce(undefined)
    const worker = new OutboxWorker(db, dispatcher, logger, ['users'], requestContext)

    worker.start()
    await vi.waitFor(() => expect(logError).toHaveBeenCalledOnce())
    worker.stop()

    expect(String(transaction.execute.mock.calls[1]?.[0])).toContain('retry_count = 1')
  })

  it('handles empty results and continues polling each configured table', async () => {
    transaction.execute
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [createRow({ id: 'message-2' })] })
      .mockResolvedValueOnce(undefined)
    const worker = new OutboxWorker(db, dispatcher, logger, ['users', 'orders'], requestContext)

    worker.start()
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce())
    worker.stop()

    expect(transact).toHaveBeenCalledTimes(3)
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ aggregateId: 'aggregate-1' }))
  })

  it('logs database errors and continues processing the next configured table', async () => {
    transaction.execute
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockResolvedValueOnce({ rows: [] })
    const worker = new OutboxWorker(db, dispatcher, logger, ['users', 'orders'], requestContext)

    worker.start()
    await vi.waitFor(() =>
      expect(logError).toHaveBeenCalledWith(
        'Outbox worker error on users_outbox_messages:',
        expect.objectContaining({ message: 'database unavailable' }),
      ),
    )
    worker.stop()

    expect(transact).toHaveBeenCalledTimes(2)
    expect(publish).not.toHaveBeenCalled()
  })
})
