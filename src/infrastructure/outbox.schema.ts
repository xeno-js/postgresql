/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
import { integer, jsonb, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core'

export const createOutboxTable = (tableName = 'default') => {
  return pgTable(`${tableName.toLowerCase()}_outbox_messages`, {
    id: uuid('id').primaryKey(),
    aggregateId: text('aggregate_id').notNull(),
    aggregateType: text('aggregate_type').notNull(),
    type: text('type').notNull(),
    payload: jsonb('payload').notNull(),
    status: text('status').notNull().default('PENDING'),
    version: integer('version').notNull(),
    correlationId: uuid('correlation_id').notNull(),
    requestId: uuid('request_id'),
    tenantId: uuid('tenant_id'),
    userId: uuid('user_id'),
    destination: text('destination'),
    retryCount: integer('retry_count').notNull().default(0),
    lastError: text('last_error'),
    publishedAt: timestamp('published_at'),
    createdAt: timestamp('created_at').defaultNow().notNull(),
  })
}
