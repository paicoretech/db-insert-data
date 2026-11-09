ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS source_uri TEXT,
    ADD COLUMN IF NOT EXISTS destination_uri TEXT;


CREATE TABLE IF NOT EXISTS cdr_ussi (
                                        record_date                TIMESTAMP NOT NULL,
                                        submit_date                TIMESTAMP,
                                        delivery_date              TIMESTAMP,
                                        id                         VARCHAR(50),
    origin_protocol            VARCHAR(20),
    origin_network_id          VARCHAR(20),
    dest_protocol              VARCHAR(20),
    dest_network_id            VARCHAR(20),
    status                     VARCHAR(50),
    comment                    VARCHAR(50),
    dialog_duration            BIGINT,
    processing_time            BIGINT,
    data_coding                INT,
    global_title               VARCHAR(50),
    global_title_indicator     VARCHAR(50),
    translation_type           VARCHAR(50),
    short_code                 VARCHAR(50),
    msisdn                     VARCHAR(50),
    short_message              TEXT,
    tcap_message_type          VARCHAR(50),
    invoke_id                  VARCHAR(50),
    ussd_ssn                   INT,
    hlr_ssn                    INT,
    msc_ssn                    INT,
    parent_id                  VARCHAR(50),
    events                     TEXT,
    error_code                 INT,
    PRIMARY KEY (id, record_date)
    ) PARTITION BY RANGE (record_date);

CREATE TABLE IF NOT EXISTS cdr_ussi_2026_03 PARTITION OF cdr_ussi
    FOR VALUES FROM ('2026-03-01') TO ('2026-03-31');

CREATE INDEX IF NOT EXISTS idx_cdr_ussi_origin_protocol ON cdr_ussi (origin_protocol);
CREATE INDEX IF NOT EXISTS idx_cdr_ussi_dest_protocol ON cdr_ussi (dest_protocol);
CREATE INDEX IF NOT EXISTS idx_cdr_ussi_msisdn ON cdr_ussi (msisdn);
CREATE INDEX IF NOT EXISTS idx_cdr_ussi_origin_network_id ON cdr_ussi (origin_network_id);
CREATE INDEX IF NOT EXISTS idx_cdr_ussi_dest_network_id ON cdr_ussi (dest_network_id);