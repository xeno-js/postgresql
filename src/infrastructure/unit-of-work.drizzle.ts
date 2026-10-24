import {
  type AggregateRoot,
  AppError,
  type Dictionary,
  Guards,
  GuidHelper,
  type IDisposable,
  type IRequestContext,
  type ITransactionState,
  type IUnitOfWork,
  type Optional,
  type OutboxMessage,
  type RequestContext,
} from '@xeno-js/core'

import type { DbContext } from './db.types'
import { createOutboxTable } from './outbox.schema'

export class PostgresqlUnitOfWork<Schema extends Dictionary>
  implements IUnitOfWork<DbContext<Schema>>, IDisposable
{
  constructor(
    private readonly _dbContext: DbContext<Schema>,
    private readonly _ttx: ITransactionState<DbContext<Schema>>,
    private readonly _requestcontext: IRequestContext<RequestContext>,
  ) {}

  async runInTransaction<T, TProps, TEvent = unknown, TId extends string = string>(
    callback: (tx: DbContext<Schema>) => Promise<T>,
    aggregates?: AggregateRoot<TProps, TEvent, TId> | AggregateRoot<TProps, TEvent, TId>[],
    signal?: Optional<AbortSignal>,
  ): Promise<T> {
    AppError.throwIfAborted(signal, 'PostgresqlUnitOfWork.runInTransaction')

    return this._dbContext.transaction(async (tx: DbContext<Schema>) => {
      this._ttx.state = tx
      try {
        const result = await callback(this._ttx.state)

        if (Guards.isDefined(aggregates)) {
          const aggregateList = Guards.isArray(aggregates) ? aggregates : [aggregates]

          for (const agg of aggregateList) {
            const events = agg.getUncommittedEvents()
            const context = this._requestcontext.getContext()

            if (!Guards.isNullOrEmpty(events)) {
              const outboxRows: OutboxMessage[] = events.map((event) => ({
                id: crypto.randomUUID(),
                aggregateId: event.aggregateId.toString(),
                aggregateType: event.aggregateType,
                type: event.eventType,
                payload: event.payload,
                createdAt: new Date(),
                correlationId: context?.tracing.correlationId ?? GuidHelper.generate(),
                requestId: context?.network.requestId,
                userId: context?.identity.userId,
                tenantId: context?.identity.tenantId,
                destination: event.destination,
                version: event.version,
                status: 'PENDING',
                retryCount: 0,
              }))

              const dynamicTableName = agg.constructor.name.toLowerCase()
              const outboxTable = createOutboxTable(dynamicTableName)

              await tx.insert(outboxTable).values(outboxRows).execute()

              agg.clearUncommittedEvents()
            }
          }
        }

        return result
      } finally {
        this._ttx.state = null
      }
    })
  }

  async dispose(): Promise<void> {
    if (Guards.isDefined(this._ttx.state)) {
      try {
        if (Guards.hasMethod(this._ttx.state, 'rollback')) {
          await (this._ttx.state as unknown as { rollback: () => Promise<void> }).rollback()
        }
      } finally {
        this._ttx.state = null
      }
    }
  }
}
