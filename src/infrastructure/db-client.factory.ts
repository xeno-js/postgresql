import type { Dictionary, IFactory, Optional } from '@xeno-js/core'
import { Guards } from '@xeno-js/core'
import { drizzle } from 'drizzle-orm/node-postgres'
import { Pool, type PoolOptions } from 'pg'

import type { DbContext } from './db.types'

/**
 * @description Factory class responsible for creating instances of IDbClient based on the provided configuration. It implements the IFactory interface, allowing for easy integration with dependency injection systems. The factory encapsulates the creation logic for the IDbClient, including the initialization of the underlying database client with the specified configuration options such as connection string and table mappings. This design promotes separation of concerns and allows for flexibility in managing IDbClient instances across the application.
 *
 * @author Xeno
 * @version 1.0.0
 * @since 2025-09-30
 * @link https://github.com/xeno-js/xeno-js
 */
export class DbClientFactory implements IFactory<PoolOptions, DbContext<Dictionary>> {
  public create(opts: Optional<PoolOptions>, env = 'development'): DbContext<Dictionary> {
    const hasConnectionString =
      Guards.isDefined(opts) && !Guards.isNullOrEmpty(opts.connectionString)
    const hasIndividualParams =
      Guards.isDefined(opts) &&
      !Guards.isNullOrEmpty(opts.host) &&
      !Guards.isNullOrEmpty(opts.database)

    if (!hasConnectionString && !hasIndividualParams) {
      throw new Error(
        '[DbClientFactory Error]: Invalid Database configuration' +
          'Please, provide a valid connection string or individual parameters (eg. "host" e "database").',
      )
    }

    const config: PoolOptions = {
      max: opts?.max ?? 20,
      idleTimeoutMillis: opts?.idleTimeoutMillis ?? 30000,
      connectionTimeoutMillis: opts?.connectionTimeoutMillis ?? 2000,
      ssl: undefined,
      maxUses: opts?.maxUses ?? 10000,
      allowExitOnIdle: opts?.allowExitOnIdle ?? true,
      maxLifetimeSeconds: opts?.maxLifetimeSeconds ?? 60 * 60,
    }

    if (env === 'production') {
      config.ssl = {
        rejectUnauthorized: true,
      }
    }

    const poolConfig: PoolOptions = {
      ...config,
      ...opts,
    }

    const pool = new Pool(poolConfig)
    pool.on('error', (err) => {
      console.error('[Database Pool Error]: Unexpected error on idle client', err)
    })
    return drizzle({ client: pool })
  }
}
