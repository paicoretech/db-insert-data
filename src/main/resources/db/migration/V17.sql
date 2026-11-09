/*
 * The SMSF message_id generator changed from currentTimeMillis+nanoTime to
 * currentTimeMillis+UUID to stop collisions under concurrency. The id is now
 * exactly 50 chars (13 + 1 + 36), which filled varchar(50) with no margin —
 * and PostgreSQL errors rather than truncates on overflow.
 */
ALTER TABLE cdr ALTER COLUMN message_id TYPE varchar(64);
ALTER TABLE cdr ALTER COLUMN parent_id  TYPE varchar(64);