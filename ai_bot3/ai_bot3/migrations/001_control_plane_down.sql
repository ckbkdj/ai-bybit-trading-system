PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS ticket_events;
DROP TABLE IF EXISTS execution_receipts;
DROP TABLE IF EXISTS ticket_claims;
DROP TABLE IF EXISTS ticket_delivery_outbox;
DROP TABLE IF EXISTS operation_tickets;
DROP TABLE IF EXISTS forecasts;
DELETE FROM schema_migrations WHERE version = 1;
PRAGMA foreign_keys=ON;
