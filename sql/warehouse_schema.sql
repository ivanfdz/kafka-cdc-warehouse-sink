-- Warehouse schema targeted by the sink.
--
-- Two reference tables describe the entities, two current-state tables hold the
-- links the sink maintains, and two append-only history tables hold the audit
-- trail. The sink never reads from the history tables, only writes to them.
--
-- The DDL stays inside the intersection of PostgreSQL and PostgreSQL-compatible
-- analytical warehouses: no ON CONFLICT, no MERGE, no partial indexes.

-- ---------------------------------------------------------------------------
-- Reference data
-- ---------------------------------------------------------------------------

-- Catalog of source systems. source_type drives the join in step 2: only orders
-- coming from an operational system are eligible for linking.
CREATE TABLE IF NOT EXISTS sources_reference (
    id              INTEGER      NOT NULL,
    source_name     VARCHAR(64)  NOT NULL,
    source_type     VARCHAR(64)  NOT NULL,
    description     VARCHAR(256),
    PRIMARY KEY (id)
);

-- Canonical customers. Rows with source_id = CATALOG_SOURCE_ID are the master
-- catalog; step 1 of the upsert backfills placeholders here.
CREATE TABLE IF NOT EXISTS customers_reference (
    id                    VARCHAR(32)  NOT NULL,
    customer_hash         VARCHAR(32)  NOT NULL,
    external_customer_id  VARCHAR(64)  NOT NULL,
    source_id             INTEGER      NOT NULL,
    source_name           VARCHAR(64)  NOT NULL,
    customer_name         VARCHAR(256),
    country_code          VARCHAR(8),
    resolution_state      VARCHAR(32),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    PRIMARY KEY (id, source_id)
);

-- Orders known to the warehouse. Loaded by the batch pipeline, read-only here.
CREATE TABLE IF NOT EXISTS orders_reference (
    id                VARCHAR(64)  NOT NULL,
    external_order_id VARCHAR(64),
    source_id         INTEGER      NOT NULL,
    source_name       VARCHAR(64),
    order_status      VARCHAR(32),
    order_date        DATE,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    PRIMARY KEY (id, source_id)
);

-- ---------------------------------------------------------------------------
-- Current state maintained by the sink
-- ---------------------------------------------------------------------------

-- Proposed order-to-customer links with their score. Refreshed per batch.
CREATE TABLE IF NOT EXISTS order_customer_candidates (
    order_source_id     INTEGER      NOT NULL,
    order_id            VARCHAR(64)  NOT NULL,
    customer_source_id  INTEGER,
    customer_id         VARCHAR(32),
    link_score          INTEGER,
    link_rating         INTEGER,
    link_method         VARCHAR(32),
    audit_action        VARCHAR(32),
    audit_action_status VARCHAR(16),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- The confirmed link. At most one row per order, enforced by the
-- delete-before-insert in step 6.
CREATE TABLE IF NOT EXISTS order_customer_links (
    order_source_id     INTEGER      NOT NULL,
    order_id            VARCHAR(64)  NOT NULL,
    customer_source_id  INTEGER,
    customer_id         VARCHAR(32),
    audit_action        VARCHAR(32),
    audit_action_at     TIMESTAMP,
    audit_action_status VARCHAR(16),
    link_score          INTEGER,
    link_rating         INTEGER,
    link_method         VARCHAR(32),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Append-only history
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS order_customer_candidates_history (
    order_source_id     INTEGER      NOT NULL,
    order_id            VARCHAR(64)  NOT NULL,
    customer_source_id  INTEGER,
    customer_id         VARCHAR(32),
    link_score          INTEGER,
    link_rating         INTEGER,
    link_method         VARCHAR(32),
    audit_action        VARCHAR(32),
    audit_action_status VARCHAR(16),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_customer_links_history (
    order_source_id     INTEGER      NOT NULL,
    order_id            VARCHAR(64)  NOT NULL,
    customer_source_id  INTEGER,
    customer_id         VARCHAR(32),
    audit_action        VARCHAR(32),
    audit_action_at     TIMESTAMP,
    audit_action_status VARCHAR(16),
    link_score          INTEGER,
    link_rating         INTEGER,
    link_method         VARCHAR(32),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Indexes that matter for the per-batch workload
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_customers_reference_external
    ON customers_reference (source_id, external_customer_id);

CREATE INDEX IF NOT EXISTS idx_order_customer_links_order
    ON order_customer_links (order_source_id, order_id);

CREATE INDEX IF NOT EXISTS idx_order_customer_candidates_order
    ON order_customer_candidates (order_source_id, order_id);

-- ---------------------------------------------------------------------------
-- Seed data for the local walkthrough
--
-- This block is the only PostgreSQL-specific part of the file: generate_series
-- is not available on the compute nodes of every analytical warehouse. It exists
-- so docker-compose has something for the sink to link against and is not part
-- of the production schema.
-- ---------------------------------------------------------------------------

INSERT INTO sources_reference (id, source_name, source_type, description)
SELECT 1, 'storefront', 'operational-web', 'Public web storefront'
WHERE NOT EXISTS (SELECT 1 FROM sources_reference WHERE id = 1);

INSERT INTO sources_reference (id, source_name, source_type, description)
SELECT 2, 'call_center', 'operational-phone', 'Phone ordering desk'
WHERE NOT EXISTS (SELECT 1 FROM sources_reference WHERE id = 2);

INSERT INTO sources_reference (id, source_name, source_type, description)
SELECT 12, 'catalog', 'master-catalog', 'Canonical customer catalog'
WHERE NOT EXISTS (SELECT 1 FROM sources_reference WHERE id = 12);

-- Twenty orders so the sample producer has something to link against.
INSERT INTO orders_reference (
    id, external_order_id, source_id, source_name, order_status, order_date,
    created_at, updated_at
)
SELECT
    'ORD-' || LPAD(CAST(seq AS VARCHAR), 4, '0'),
    'EXT-' || LPAD(CAST(seq AS VARCHAR), 6, '0'),
    1,
    'storefront',
    'CONFIRMED',
    DATE '2026-01-05',
    TIMESTAMP '2026-01-05 09:00:00',
    TIMESTAMP '2026-01-05 09:00:00'
FROM generate_series(1, 20) AS seq
WHERE NOT EXISTS (
    SELECT 1 FROM orders_reference WHERE source_id = 1
);
