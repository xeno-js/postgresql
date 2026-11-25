import type {
  Dictionary,
  IDomainEvent,
  IRequestContext,
  ITransactionState,
  OutboxMessage,
  RequestContext,
} from '@xeno-js/core'
import { AggregateRoot as AggregateRootBase } from '@xeno-js/core'
import { getTableName } from 'drizzle-orm'
import type { AnyPgTable } from 'drizzle-orm/pg-core'
import type { Mock } from 'vitest'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { DbContext } from '../db.types'
import { PostgresqlUnitOfWork } from '../unit-of-work.drizzle'

interface TransactionStub {
  insert: Mock<
    (table: AnyPgTable) => {
      values: Mock<(rows: OutboxMessage[]) => { execute: Mock<() => Promise<void>> }>
    }
  >
  rollback?: Mock<() => Promise<void>>
}

type TransactionCallback = <T>(callback: (transaction: TransactionStub) => Promise<T>) => Promise<T>

class TestAggregate extends AggregateRootBase<{ name: string }, { name: string }> {
  public lastAppliedEvent: IDomainEvent<{ name: string }> | null = null

  constructor(withEvent = true) {
    super({ name: 'Ada' }, '11111111-1111-4111-8111-111111111111')

    if (withEvent) {
      this.raise({
        aggregateType: 'TestAggregate',
        eventType: 'test.created',
        payload: { name: 'Ada' },
        destination: 'test-topic',
      })
    }
  }

  protected apply(event: IDomainEvent<{ name: string }>, _isNew: boolean): void {
    this.lastAppliedEvent = event
  }
}

describe('PostgresqlUnitOfWork', () => {
  let execute: Mock<() => Promise<void>>
  let insert: TransactionStub['insert']
  let transaction: TransactionStub
  let transact: Mock<TransactionCallback>
  let db: DbContext<Dictionary>
  let state: ITransactionState<DbContext<Dictionary>>
  let requestContext: IRequestContext<RequestContext>
  let getContext: Mock<IRequestContext<RequestContext>['getContext']>
  let context: RequestContext
  let insertedRows: OutboxMessage[]

  beforeEach(() => {
    insertedRows = []
    execute = vi.fn<() => Promise<void>>().mockResolvedValue()
    insert = vi.fn((table: AnyPgTable) => {
      expect(getTableName(table)).toBe('testaggregate_outbox_messages')
      return {
        values: vi.fn((rows: OutboxMessage[]) => {
          insertedRows.push(...rows)
          return { execute }
        }),
      }
    })
    transaction = { insert }
    transact = vi.fn(async <T>(callback: (tx: TransactionStub) => Promise<T>) =>
      callback(transaction),
    )
    db = { transaction: transact } as unknown as DbContext<Dictionary>
    state = { state: null }
    context = {
      identity: {
        userId: '11111111-1111-4111-8111-111111111111',
        tenantId: '22222222-2222-4222-8222-222222222222',
      },
      network: {
        requestId: '33333333-3333-4333-8333-333333333333',
        clientIp: '127.0.0.1',
        path: '/test',
        formatIndicator: 'application/json',
      },
      tracing: {
        correlationId: '44444444-4444-4444-8444-444444444444',
        startTime: Date.now(),
        spanId: '55555555-5555-4555-8555-555555555555',
      },
    }
    getContext = vi.fn(() => context)
    requestContext = { getContext } as unknown as IRequestContext<RequestContext>
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('runs the callback in the database transaction and clears active transaction state', async () => {
    const resultValue = { saved: true }
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)
    let callbackTransaction: DbContext<Dictionary> | undefined

    const result = await unitOfWork.runInTransaction(async (tx) => {
      callbackTransaction = tx
      expect(state.state).toBe(transaction)
      return resultValue
    })

    expect(transact).toHaveBeenCalledOnce()
    expect(callbackTransaction).toBe(transaction)
    expect(result).toBe(resultValue)
    expect(state.state).toBeNull()
    expect(insert).not.toHaveBeenCalled()
  })

  it('clears transaction state and propagates callback errors', async () => {
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)
    const failure = new Error('callback failed')

    await expect(
      unitOfWork.runInTransaction(async () => {
        throw failure
      }),
    ).rejects.toBe(failure)

    expect(state.state).toBeNull()
    expect(insert).not.toHaveBeenCalled()
  })

  it('rejects an aborted signal before opening a database transaction', async () => {
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)
    const controller = new AbortController()
    controller.abort()

    await expect(
      unitOfWork.runInTransaction(async () => 'unreachable', undefined, controller.signal),
    ).rejects.toThrow()

    expect(transact).not.toHaveBeenCalled()
    expect(state.state).toBeNull()
  })

  it('persists domain events to the PostgreSQL outbox and clears the aggregate events', async () => {
    const aggregate = new TestAggregate()
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.runInTransaction(async () => 'done', aggregate)

    expect(insert).toHaveBeenCalledOnce()
    expect(execute).toHaveBeenCalledOnce()
    expect(insertedRows).toHaveLength(1)
    expect(insertedRows[0]).toEqual(
      expect.objectContaining({
        aggregateId: '11111111-1111-4111-8111-111111111111',
        aggregateType: 'TestAggregate',
        type: 'test.created',
        payload: { name: 'Ada' },
        status: 'PENDING',
        retryCount: 0,
        correlationId: context.tracing.correlationId,
        requestId: context.network.requestId,
        userId: context.identity.userId,
        tenantId: context.identity.tenantId,
        destination: 'test-topic',
      }),
    )
    expect(typeof insertedRows[0]?.version).toBe('number')
    expect(insertedRows[0]?.createdAt).toBeInstanceOf(Date)
    expect(aggregate.getUncommittedEvents()).toHaveLength(0)
    expect(state.state).toBeNull()
  })

  it('handles aggregate arrays and skips aggregates without uncommitted events', async () => {
    const emptyAggregate = new TestAggregate(false)
    const aggregate = new TestAggregate()
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.runInTransaction(async () => 'done', [emptyAggregate, aggregate])

    expect(insert).toHaveBeenCalledOnce()
    expect(insertedRows).toHaveLength(1)
    expect(emptyAggregate.getUncommittedEvents()).toHaveLength(0)
    expect(aggregate.getUncommittedEvents()).toHaveLength(0)
  })

  it('generates a correlation ID when request context is unavailable', async () => {
    getContext.mockReturnValue(undefined)
    const aggregate = new TestAggregate()
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.runInTransaction(async () => 'done', aggregate)

    expect(insertedRows[0]?.correlationId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    )
    expect(insertedRows[0]?.requestId).toBeUndefined()
    expect(insertedRows[0]?.userId).toBeUndefined()
    expect(insertedRows[0]?.tenantId).toBeUndefined()
  })

  it('preserves aggregate events and clears transaction state if outbox insertion fails', async () => {
    execute.mockRejectedValueOnce(new Error('outbox insert failed'))
    const aggregate = new TestAggregate()
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await expect(unitOfWork.runInTransaction(async () => 'done', aggregate)).rejects.toThrow(
      'outbox insert failed',
    )

    expect(aggregate.getUncommittedEvents()).toHaveLength(1)
    expect(state.state).toBeNull()
  })

  it('rolls back an active transaction when disposed', async () => {
    const rollback = vi.fn<() => Promise<void>>().mockResolvedValue()
    const activeTransaction = { rollback }
    state = { state: activeTransaction } as unknown as ITransactionState<DbContext<Dictionary>>
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.dispose()

    expect(rollback).toHaveBeenCalledOnce()
    expect(state.state).toBeNull()
  })

  it('clears transaction state even when rollback during disposal fails', async () => {
    const failure = new Error('rollback failed')
    const rollback = vi.fn<() => Promise<void>>().mockRejectedValue(failure)
    state = { state: { rollback } } as unknown as ITransactionState<DbContext<Dictionary>>
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await expect(unitOfWork.dispose()).rejects.toBe(failure)

    expect(rollback).toHaveBeenCalledOnce()
    expect(state.state).toBeNull()
  })

  it('clears transaction state during disposal when rollback is unavailable', async () => {
    const activeTransaction = {}
    state = { state: activeTransaction } as unknown as ITransactionState<DbContext<Dictionary>>
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.dispose()

    expect(state.state).toBeNull()
  })

  it('does nothing during disposal if there is no active transaction', async () => {
    const unitOfWork = new PostgresqlUnitOfWork(db, state, requestContext)

    await unitOfWork.dispose()

    expect(state.state).toBeNull()
    expect(transact).not.toHaveBeenCalled()
  })
})
