import type {
  IConfigurationService,
  IServiceContainer,
  IServiceScope,
  XenoAppBuilder,
  XenoRegistry,
} from '@xeno-js/core'
import { TOKENS } from '@xeno-js/core'
import type { PoolOptions } from 'pg'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { OutboxWorker } from '../background.worker'
import type { DbContext, DbTransaction } from '../db.types'
import { withPostgresql } from '../postgresql.plugin'
import { PostgresqlUnitOfWork } from '../unit-of-work.drizzle'

const { createMock, mockLoggerConfigure, MockDbModuleClass } = vi.hoisted(() => ({
  createMock: vi.fn(),
  mockLoggerConfigure: vi.fn(),
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
  LoggerModule: class {
    public configure = mockLoggerConfigure
  },
  Guards: {
    isDefined: vi.fn((value: unknown) => value !== null && value !== undefined),
    isArray: vi.fn(Array.isArray),
    isNullOrEmpty: vi.fn(
      (value: unknown) =>
        value === null ||
        value === undefined ||
        value === '' ||
        (Array.isArray(value) && value.length === 0),
    ),
  },
  TOKENS: {
    DB_CONTEXT: 'DB_CONTEXT',
    LOGGER: 'LOGGER',
    EVENT_DISPATCHER: 'EVENT_DISPATCHER',
    REQUEST_CONTEXT: 'REQUEST_CONTEXT',
    BACKGROUND_WORKER: 'BACKGROUND_WORKER',
    TRANSACTION_STATE: 'TRANSACTION_STATE',
  },
  definePlugin: vi.fn((_kind: string, plugin: (...args: never[]) => unknown) => {
    Object.defineProperty(plugin, '__kind', { value: _kind })
    return plugin
  }),
}))

type Registry = XenoRegistry<DbContext, DbTransaction>
type TestBuilder = XenoAppBuilder<Registry>
type TestContainer = IServiceContainer<Registry>
type Setup = NonNullable<Parameters<typeof withPostgresql>[0]>

interface RegisteredModule {
  name: string
  factory: () => Promise<{ configure: (container: TestContainer) => Promise<void> }>
  options: unknown
  priority: number
}

function makeBuilder() {
  const addModule = vi.fn(
    (_name: string, _factory: () => Promise<unknown>, _options: unknown, _priority: number) =>
      undefined,
  )
  const builder = { addModule } as unknown as TestBuilder

  return { builder, addModule }
}

function registeredModules(
  addModule: ReturnType<typeof makeBuilder>['addModule'],
): RegisteredModule[] {
  return addModule.mock.calls.map(([name, factory, options, priority]) => ({
    name,
    factory: factory as RegisteredModule['factory'],
    options,
    priority,
  }))
}

function makeContainer(
  db: { execute: ReturnType<typeof vi.fn> } = { execute: vi.fn().mockResolvedValue(undefined) },
) {
  const scopeResolve = vi.fn(() => db)
  const scopeDispose = vi.fn().mockResolvedValue(undefined)
  const scope = {
    resolve: scopeResolve,
    dispose: scopeDispose,
  }
  const createScope = vi.fn(() => scope)
  const subscribe = vi.fn()
  const dispatcher = { subscribe }
  const addSingleton =
    vi.fn<(token: unknown, factory: (scope: IServiceScope<Registry>) => unknown) => unknown>()
  const isRegistered = vi.fn().mockReturnValue(true)
  const containerResolve = vi.fn((token: string) => {
    if (token === TOKENS.EVENT_DISPATCHER) return dispatcher
    return {}
  })
  const container = {
    createScope,
    isRegistered,
    addSingleton,
    resolve: containerResolve,
  }

  return {
    addSingleton,
    container: container as unknown as TestContainer,
    containerResolve,
    createScope,
    db,
    dispatcher,
    isRegistered,
    scope,
    scopeDispose,
    scopeResolve,
    subscribe,
  }
}

describe('withPostgresql', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.spyOn(console, 'info').mockImplementation(() => undefined)
    vi.spyOn(console, 'error').mockImplementation(() => undefined)
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

  it('registers an outbox module and a unit-of-work factory when outbox is enabled', () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, name: 'Order' }
    }

    withPostgresql(setup)(builder, config)

    expect(addModule).toHaveBeenCalledWith('OUTBOX', expect.any(Function), {}, 50)
    const modules = registeredModules(addModule)
    const dbOptions = modules.find(({ name }) => name === 'PostgresDbModule')?.options as {
      client: unknown
      uow: (scope: IServiceScope<Registry>) => unknown
    }
    expect(dbOptions.uow).toBeTypeOf('function')
    const dbContext = {}
    const transactionState = {}
    const requestContext = {}
    const resolve = vi.fn((token: string) => {
      if (token === TOKENS.DB_CONTEXT) return dbContext
      if (token === TOKENS.TRANSACTION_STATE) return transactionState
      return requestContext
    })
    const scope = {
      resolve,
    } as unknown as IServiceScope<Registry>

    expect(dbOptions.uow(scope)).toBeInstanceOf(PostgresqlUnitOfWork)
    expect(resolve).toHaveBeenCalledWith(TOKENS.DB_CONTEXT)
    expect(resolve).toHaveBeenCalledWith(TOKENS.TRANSACTION_STATE)
    expect(resolve).toHaveBeenCalledWith(TOKENS.REQUEST_CONTEXT)
  })

  it.each([undefined, '', []])(
    'rejects enabled outbox configuration without a name (%s)',
    (name) => {
      const config = new ConfigurationServiceMock()
      const { builder, addModule } = makeBuilder()
      const setup: Setup = (options) => {
        options.connectionString = 'postgres://localhost:5432/test'
        options.outbox = { enable: true, name }
      }

      expect(() => withPostgresql(setup)(builder, config)).toThrow('outbox name is required.')
      expect(addModule).not.toHaveBeenCalled()
    },
  )

  it('creates outbox tables for each configured name and disposes the scope', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, name: ['Order', 'Product'] }
    }
    withPostgresql(setup)(builder, config)

    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const { container, db, scope, scopeResolve } = makeContainer()
    await (await outboxModule.factory()).configure(container)

    expect(db.execute).toHaveBeenCalledTimes(2)
    expect(scopeResolve).toHaveBeenCalledTimes(2)
    expect(scope.dispose).toHaveBeenCalledOnce()
    expect(console.info).toHaveBeenCalledWith(
      '  [Postgresql Plugin] Outbox table checked/created successfully.',
    )
  })

  it('creates one outbox table from a scalar name', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, name: 'Order' }
    }
    withPostgresql(setup)(builder, config)

    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const { container, db, scope } = makeContainer()
    await (await outboxModule.factory()).configure(container)

    expect(db.execute).toHaveBeenCalledOnce()
    expect(scope.dispose).toHaveBeenCalledOnce()
  })

  it('skips initialization when the outbox name is absent at configuration time', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    let configuredOptions: Parameters<Setup>[0] | undefined
    const setup: Setup = (options) => {
      configuredOptions = options
      options.outbox = { enable: true, name: 'Order' }
    }
    withPostgresql(setup)(builder, config)
    if (configuredOptions === undefined) throw new Error('Setup options were not captured')
    configuredOptions.outbox = { enable: true }
    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const { container, createScope, scopeDispose } = makeContainer()

    await (await outboxModule.factory()).configure(container)

    expect(createScope).not.toHaveBeenCalled()
    expect(scopeDispose).not.toHaveBeenCalled()
  })

  it('registers the outbox worker, initializes the default logger, and subscribes configured handlers', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const handler = { eventType: 'order.created', handle: vi.fn() }
    const handlerFactory = vi.fn(() => handler)
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, worker: true, name: 'Order', handlers: [handlerFactory] }
    }
    withPostgresql(setup)(builder, config)
    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const { addSingleton, container, isRegistered, scopeResolve, subscribe } = makeContainer()
    isRegistered.mockReturnValue(false)

    await (await outboxModule.factory()).configure(container)

    expect(mockLoggerConfigure).toHaveBeenCalledOnce()
    expect(addSingleton).toHaveBeenCalledWith(TOKENS.BACKGROUND_WORKER, expect.any(Function))
    expect(handlerFactory).toHaveBeenCalledWith(container)
    expect(subscribe).toHaveBeenCalledWith('order.created', handler)
    expect(scopeResolve).toHaveBeenCalledTimes(2)

    const workerFactory = addSingleton.mock.calls[0]?.[1]
    if (workerFactory === undefined) throw new Error('Background worker factory was not registered')
    const workerScope = container as unknown as IServiceScope<Registry>
    const worker = workerFactory(workerScope)
    expect(worker).toBeInstanceOf(OutboxWorker)
  })

  it('registers the worker without initializing a logger or dispatcher when handlers are absent', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, worker: true, name: 'Order' }
    }
    withPostgresql(setup)(builder, config)
    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const { addSingleton, container, containerResolve, isRegistered } = makeContainer()

    await (await outboxModule.factory()).configure(container)

    expect(isRegistered).toHaveBeenCalledWith(TOKENS.LOGGER)
    expect(mockLoggerConfigure).not.toHaveBeenCalled()
    expect(addSingleton).toHaveBeenCalledOnce()
    expect(containerResolve).not.toHaveBeenCalled()
  })

  it('logs and rethrows table initialization errors while disposing the scope', async () => {
    createMock.mockReturnValue({ kind: 'configured-client' })
    const config = new ConfigurationServiceMock()
    const { builder, addModule } = makeBuilder()
    const setup: Setup = (options) => {
      options.connectionString = 'postgres://localhost:5432/test'
      options.outbox = { enable: true, name: 'Order' }
    }
    withPostgresql(setup)(builder, config)

    const outboxModule = registeredModules(addModule).find(({ name }) => name === 'OUTBOX')
    if (outboxModule === undefined) throw new Error('Outbox module was not registered')
    const failure = new Error('database unavailable')
    const { container, scope } = makeContainer({ execute: vi.fn().mockRejectedValue(failure) })

    await expect((await outboxModule.factory()).configure(container)).rejects.toBe(failure)
    expect(console.error).toHaveBeenCalledWith(
      '  [Postgresql Plugin] Failed to initialize outbox table:',
      failure,
    )
    expect(scope.dispose).toHaveBeenCalledOnce()
  })
})
