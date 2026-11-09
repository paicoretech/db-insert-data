ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS message_priority VARCHAR(50);
