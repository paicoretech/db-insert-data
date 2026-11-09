ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS origination_network_name VARCHAR(50);

ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS destination_network_name VARCHAR(50);