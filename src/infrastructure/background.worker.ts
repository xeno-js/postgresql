/* eslint-disable @typescript-eslint/no-misused-promises */
import type {
  Dictionary,
  IBackgroundWorker,
  IEventDispatcher,
  ILogger,
  IRequestContext,
  RequestContext,
} from '@xeno-js/core'
import { Guards, GuidHelper, StringHelper } from '@xeno-js/core'
import { sql } from 'drizzle-orm'

import type { DbContext } from './db.types'

export class OutboxWorker<Schema extends Dictionary> implements IBackgroundWorker {
  private _isRunning = false
  private _timer: NodeJS.Timeout | null = null

  constructor(
    private readonly _db: DbContext<Schema>,
    private readonly _dispatcher: IEventDispatcher,
    private readonly _logger: ILogger,
    private readonly _tableNames: string[],
    private readonly _requestContext: IRequestContext<RequestContext>, // <-- AGGIUNTO
  ) {}

  public start(intervalMs = 5000): void {
    if (this._isRunning) return
    this._isRunning = true
    this._logger.info(
      `Outbox Worker started. Polling ${this._tableNames.length} tables every ${intervalMs} ms.`,
    )
    const pollLoop = async (): Promise<void> => {
      if (!this._isRunning) return
      await this.processOutbox()
      if (this._isRunning) {
        this._timer = setTimeout(pollLoop, intervalMs)
      }
    }
    void pollLoop()
  }

  public stop(): void {
    this._isRunning = false
    if (Guards.isDefined(this._timer)) clearInterval(this._timer)
  }

  private async processOutbox(): Promise<void> {
    for (const name of this._tableNames) {
      const tableName = `${name}_outbox_messages`
      await this.processTable(tableName)
    }
  }

  private async processTable(tableName: string): Promise<void> {
    try {
      const rows = await this._db.transaction(async (tx) => {
        const messages = await tx.execute(
          sql.raw(`
                     SELECT id, type, payload, aggregate_id, retry_count 
                     FROM ${tableName} 
                     WHERE status = 'PENDING' 
                     ORDER BY created_at ASC LIMIT 50 
                     FOR UPDATE SKIP LOCKED
                `),
        )
        return messages.rows
      })
      if (Guards.isNullOrEmpty(rows)) return

      for (const row of rows) {
        const messageId = String(row['id'])
        const retryCount = Number(row['retry_count'] ?? 0)

        const event = {
          eventType: String(row['type']),
          aggregateId: String(row['aggregate_id']),
          payload: Guards.isString(row['payload'])
            ? StringHelper.safeParse(row['payload'])
            : row['payload'],
        }

        const syntheticContext: RequestContext = {
          identity: { userId: undefined, tenantId: undefined },
          network: {
            requestId: GuidHelper.generate(),
            clientIp: '127.0.0.1',
            path: 'internal://outbox-worker',
            formatIndicator: 'application/json',
          },
          tracing: {
            correlationId: GuidHelper.generate(),
            startTime: Date.now(),
            spanId: GuidHelper.generate(),
          },
        }

        const result = await this._requestContext.runAsync(syntheticContext, async () => {
          return await this._dispatcher.publish(event)
        })

        await this._db.transaction(async (tx) => {
          if (result.isOk()) {
            await tx.execute(
              sql.raw(`
                            UPDATE ${tableName} 
                            SET status = 'PUBLISHED', 
                            published_at = NOW() 
                            WHERE id = '${messageId}'
                        `),
            )
          } else {
            const errorMsg = String(result.getErrorOrThrow().message ?? 'Unknown error').replace(
              /'/g,
              "''",
            )
            const newRetryCount = retryCount + 1
            const newStatus = newRetryCount >= 3 ? 'FAILED' : 'PENDING'

            await tx.execute(
              sql.raw(`
                            UPDATE ${tableName} 
                            SET status = '${newStatus}', 
                            retry_count = ${newRetryCount}, 
                            last_error = '${errorMsg}' 
                            WHERE id = '${messageId}' 
                        `),
            )

            this._logger.error(
              `Outbox routing failed for ${messageId} (Attempt ${newRetryCount}):`,
              result.getErrorOrThrow(),
            )
          }
        })
      }
    } catch (error) {
      this._logger.error(`Outbox worker error on ${tableName}:`, error)
    }
  }
}
