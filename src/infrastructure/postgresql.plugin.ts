import type {
  DbPlugin,
  Dictionary,
  IConfigurationService,
  IEventHandler,
  IModule,
  IServiceScope,
  Optional,
  SetupAction,
  XenoRegistry,
} from '@xeno-js/core'
import { DbModule, definePlugin, Guards, LoggerModule, TOKENS } from '@xeno-js/core'
import { sql } from 'drizzle-orm'
import type { PoolOptions } from 'pg'

import { OutboxWorker } from './background.worker'
import type { DbContext, DbTransaction } from './db.types'
import { DbClientFactory } from './db-client.factory'
import { PostgresqlUnitOfWork } from './unit-of-work.drizzle'

/**
 * @description This function is a plugin for the Xeno.JS application builder that sets up the PostgreSQL database module.
 * It takes an optional PoolOptions object as a parameter and returns a function that configures the application builder.
 * The returned function adds a new module to the application builder, which is an instance of the DbModule class.
 * The DbModule class is responsible for managing the database connection and providing access to the database client.
 */
export const withPostgresql = <
  TRegistry extends XenoRegistry<DbContext, DbTransaction> = XenoRegistry<DbContext, DbTransaction>,
>(
  setupAction?: Optional<
    SetupAction<
      PoolOptions & {
        outbox?: {
          enable: boolean
          worker?: boolean
          name?: string | string[]
          handlers?: ((scope: IServiceScope<TRegistry>) => IEventHandler<unknown>)[]
        }
      },
      IConfigurationService
    >
  >,
): DbPlugin<TRegistry> => {
  return definePlugin('db', (builder, config) => {
    const opts = {} as PoolOptions & {
      outbox?: {
        enable: boolean
        worker?: boolean
        name?: string | string[]
        handlers?: (<T>(scope: IServiceScope<TRegistry>) => IEventHandler<T>)[]
      }
    }

    if (Guards.isDefined(setupAction)) setupAction(opts, config)

    const client = new DbClientFactory().create(opts, config.getEnvironment())

    const dbModule: IModule<TRegistry> = {
      async configure(container) {
        const array = opts.outbox?.name
        if (!Guards.isDefined(array)) return

        const scope = container.createScope()
        try {
          const names = Guards.isArray(array) ? array : [array]
          for (const name of names) {
            const tableName = `${name?.toLowerCase()}_outbox_messages`
            const db = scope.resolve(TOKENS.DB_CONTEXT)
            await db.execute(sql`
      CREATE TABLE IF NOT EXISTS ${sql.identifier(tableName)} (
          id UUID PRIMARY KEY,
          aggregate_id TEXT NOT NULL,
          aggregate_type TEXT NOT NULL,
          type TEXT NOT NULL,
          payload JSONB NOT NULL,
          status TEXT NOT NULL DEFAULT 'PENDING',
          version INT NOT NULL,
          correlation_id UUID NOT NULL,
          request_id UUID,
          tenant_id UUID,
          user_id UUID,
          destination TEXT,
          retry_count INT NOT NULL DEFAULT 0,
          last_error TEXT,
          published_at TIMESTAMP,
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
      );
  `)
          }

          if (Guards.isDefined(opts.outbox?.worker) && opts.outbox.worker) {
            if (!container.isRegistered(TOKENS.LOGGER)) {
              await new LoggerModule<TRegistry>().configure(container, {
                console: true,
                builder,
                config,
              })
            }

            const db = scope.resolve(TOKENS.DB_CONTEXT)

            container.addSingleton(
              TOKENS.BACKGROUND_WORKER,
              (c) =>
                new OutboxWorker<Dictionary>(
                  db,
                  c.resolve(TOKENS.EVENT_DISPATCHER),
                  c.resolve(TOKENS.LOGGER),
                  names,
                  c.resolve(TOKENS.REQUEST_CONTEXT),
                ),
            )
            if (
              Guards.isDefined(opts.outbox?.handlers) &&
              !Guards.isNullOrEmpty(opts.outbox.handlers)
            ) {
              const dispatcher = container.resolve(TOKENS.EVENT_DISPATCHER)

              for (const handlerFactory of opts.outbox.handlers) {
                const handler = handlerFactory(container)
                dispatcher.subscribe(handler.eventType, handler)
              }
            }
          }
          console.info('  [Postgresql Plugin] Outbox table checked/created successfully.')
          return
        } catch (error) {
          console.error('  [Postgresql Plugin] Failed to initialize outbox table:', error)
          throw error
        } finally {
          await scope.dispose()
        }
      },
    }

    if (Guards.isDefined(opts.outbox) && opts.outbox.enable) {
      if (Guards.isNullOrEmpty(opts.outbox.name))
        throw new Error(
          'outbox name is required. Please, set outbox.name in your configuration file. The provided name must be reflect the name of the aggregate root. (e.g., outox = {name: "Order"} or outbox = {name: ["Order", "Product"]})',
        )

      builder.addModule('OUTBOX', async () => dbModule, {}, 50)
    }

    const uow =
      Guards.isDefined(opts.outbox) && opts.outbox.enable
        ? (c: IServiceScope<TRegistry>) =>
            new PostgresqlUnitOfWork(
              c.resolve(TOKENS.DB_CONTEXT),
              c.resolve(TOKENS.TRANSACTION_STATE),
              c.resolve(TOKENS.REQUEST_CONTEXT),
            )
        : undefined

    builder.addModule(
      'PostgresDbModule',
      async () => {
        return new DbModule<TRegistry, DbContext, DbTransaction>()
      },
      { client, uow },
      2,
    )
  })
}
