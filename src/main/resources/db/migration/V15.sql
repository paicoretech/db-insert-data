CREATE INDEX IF NOT EXISTS idx_cdr_comment_record_date ON cdr (comment, record_date DESC);
