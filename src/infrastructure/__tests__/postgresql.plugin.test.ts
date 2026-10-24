import type { IConfigurationService, XenoAppBuilder, XenoRegistry } from '@xeno-js/core'
import type { PoolOptions } from 'pg'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { DbContext, DbTransaction } from '../db.types'
import { withPostgresql } from '../postgresql.plugin'

const { createMock, MockDbModuleClass } = vi.hoisted(() => ({
  createMock: vi.fn(),
  MockDbModuleClass: class MockDbModuleImplementation {
    public readonly name = 'MockDbModule'
  },
}))

class ConfigurationServiceMock implements IConfigurationService {
  public get = vi.fn().mockReturnValue('postgres://localhost:5432/test')
  public getEnvironment(): string {
    return 'test'
  }
  public getNumber = vi.fn().mockReturnValue(0)
  public getBoolean = vi.fn().mockReturnValue(false)
  public getOrThrow = vi.fn().mockReturnValue('default')
}

vi.mock('../db-client.factory', () => ({
  DbClientFactory: class {
    public create = createMock
  },
}))

vi.mock('@xeno-js/core', () => ({
  DbModule: MockDbModuleClass,
  Guards: {
    isDefined: vi.fn((value: unknown) => value !== null && value !== undefined),
  },
  definePlugin: vi.fn((_kind: string, plugin: (...args: never[]) => unknown) => {
    Object.defineProperty(plugin, '__kind', { value: _kind })
    return plugin
  }),
}))

type Registry = XenoRegistry<DbContext, DbTransaction>
type TestBuilder = XenoAppBuilder<Registry>

function makeBuilder() {
  const addModule = vi.fn(
    (_name: string, _factory: () => Promise<unknown>, _options: unknown, _priority: number) =>
      undefined,
  )
  const builder = { addModule } as unknown as TestBuilder

  return { builder, addModule }
}

describe('withPostgresql', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('creates the database client from the configuration service and registers the module', async () => {
    const client = { connectionString: 'postgres://localhost:5432/test', kind: 'postgres-client' }
    createMock.mockReturnValue(client)
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const plugin = withPostgresql()

    expect(plugin).toBeTypeOf('function')
    expect(plugin.__kind).toBe('db')
    expect(plugin(builder, config)).toBeUndefined()

    expect(createMock).toHaveBeenCalledWith({}, 'test')
    expect(addModule).toHaveBeenCalledOnce()
    expect(addModule).toHaveBeenCalledWith('PostgresDbModule', expect.any(Function), { client }, 2)

    const factory = addModule.mock.calls[0]?.[1]
    expect(factory).toBeTypeOf('function')
    await expect(factory?.()).resolves.toBeInstanceOf(MockDbModuleClass)
  })

  it('passes mutable pool options and configuration to the optional setup action', () => {
    const client = { kind: 'configured-client' }
    createMock.mockReturnValue(client)
    const config = new ConfigurationServiceMock()
    const setup = vi.fn((options: PoolOptions, _config: IConfigurationService) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.max = 12
      options.idleTimeoutMillis = 30000
      options.allowExitOnIdle = true
    })
    const { builder, addModule } = makeBuilder()

    withPostgresql(setup)(builder, config)

    expect(setup).toHaveBeenCalledOnce()
    expect(setup).toHaveBeenCalledWith(
      {
        connectionString: 'postgres://localhost:5432/test',
        max: 12,
        idleTimeoutMillis: 30000,
        allowExitOnIdle: true,
      },
      config,
    )
    expect(createMock).toHaveBeenCalledWith(
      {
        connectionString: 'postgres://localhost:5432/test',
        max: 12,
        idleTimeoutMillis: 30000,
        allowExitOnIdle: true,
      },
      'test',
    )
    expect(addModule).toHaveBeenCalledWith('PostgresDbModule', expect.any(Function), { client }, 2)
  })
})
