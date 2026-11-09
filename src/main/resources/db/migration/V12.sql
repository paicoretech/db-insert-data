ALTER TABLE public.cdr
ALTER COLUMN message_type type varchar(50) using message_type::varchar(50);