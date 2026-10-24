import type { PoolOptions } from 'pg'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { poolMock, createPoolInstance, drizzleMock } = vi.hoisted(() => ({
  poolMock: vi.fn(),
  createPoolInstance: vi.fn(),
  drizzleMock: vi.fn(),
}))

vi.mock('pg', () => ({
  Pool: class {
    public constructor(...args: [PoolOptions?]) {
      poolMock(...args)
      return createPoolInstance() as object
    }
  },
}))

vi.mock('drizzle-orm/node-postgres', () => ({
  drizzle: drizzleMock,
}))

import { DbClientFactory } from '../db-client.factory'

describe('DbClientFactory', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
    poolMock.mockReset()
    createPoolInstance.mockReset()
    drizzleMock.mockReset()
  })

  it('creates a configured Pool, logs idle errors, and returns the drizzle client', () => {
    const opts: PoolOptions = {
      connectionString: 'postgres://localhost:5432/test',
      max: 5,
      idleTimeoutMillis: 25000,
      connectionTimeoutMillis: 3000,
      maxUses: 500,
      allowExitOnIdle: false,
      maxLifetimeSeconds: 120,
    }
    const poolInstance = {
      on: vi.fn((_event: 'error', _listener: (error: Error) => void) => undefined),
    }
    const dbClient = { query: vi.fn() }
    const idleError = new Error('idle connection error')
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined)

    createPoolInstance.mockReturnValue(poolInstance)
    drizzleMock.mockReturnValue(dbClient)

    const factory = new DbClientFactory()
    const result = factory.create(opts, 'production')
    const errorListener = poolInstance.on.mock.calls[0]?.[1]

    expect(poolMock).toHaveBeenCalledOnce()
    expect(poolMock).toHaveBeenCalledWith({
      ...opts,
      ssl: { rejectUnauthorized: true },
    })
    expect(poolInstance.on).toHaveBeenCalledOnce()
    expect(poolInstance.on).toHaveBeenCalledWith('error', expect.any(Function))
    expect(errorListener).toBeTypeOf('function')
    errorListener?.(idleError)
    expect(consoleError).toHaveBeenCalledWith(
      '[Database Pool Error]: Unexpected error on idle client',
      idleError,
    )
    expect(drizzleMock).toHaveBeenCalledOnce()
    expect(drizzleMock).toHaveBeenCalledWith({ client: poolInstance })
    expect(result).toBe(dbClient)
  })

  it('uses default Pool options with a valid connection string', () => {
    const poolInstance = {
      on: vi.fn((_event: 'error', _listener: (error: Error) => void) => undefined),
    }
    const dbClient = { transaction: vi.fn() }

    createPoolInstance.mockReturnValue(poolInstance)
    drizzleMock.mockReturnValue(dbClient)

    const factory = new DbClientFactory()
    const result = factory.create({
      connectionString: 'postgres://localhost:5432/test',
    } as unknown as PoolOptions)

    expect(poolMock).toHaveBeenCalledOnce()
    expect(poolMock).toHaveBeenCalledWith({
      connectionString: 'postgres://localhost:5432/test',
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
      ssl: undefined,
      maxUses: 10000,
      allowExitOnIdle: true,
      maxLifetimeSeconds: 3600,
    })
    expect(poolInstance.on).toHaveBeenCalledWith('error', expect.any(Function))
    expect(drizzleMock).toHaveBeenCalledOnce()
    expect(drizzleMock).toHaveBeenCalledWith({ client: poolInstance })
    expect(result).toBe(dbClient)
  })

  it('rejects missing database configuration', () => {
    const factory = new DbClientFactory()

    expect(() => factory.create(undefined)).toThrow(
      '[DbClientFactory Error]: Invalid Database configuration',
    )
    expect(poolMock).not.toHaveBeenCalled()
    expect(drizzleMock).not.toHaveBeenCalled()
  })

  it('rejects individual connection options without a database', () => {
    const factory = new DbClientFactory()

    expect(() => factory.create({ host: 'localhost' } as unknown as PoolOptions)).toThrow(
      '[DbClientFactory Error]: Invalid Database configuration',
    )
    expect(poolMock).not.toHaveBeenCalled()
    expect(drizzleMock).not.toHaveBeenCalled()
  })
  it('rejects options without connection details', () => {
    const factory = new DbClientFactory()

    expect(() => factory.create({} as unknown as PoolOptions)).toThrow(
      '[DbClientFactory Error]: Invalid Database configuration',
    )
    expect(poolMock).not.toHaveBeenCalled()
    expect(drizzleMock).not.toHaveBeenCalled()
  })
})
