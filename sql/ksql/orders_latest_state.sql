-- Optional stream-processing layer.
--
-- The sink can read raw CDC topics directly. This layer sits in between when a
-- downstream consumer needs the *latest state* of each key rather than the
-- change log: it collapses the change stream into a compacted table, drops rows
-- whose last operation was a delete, and rolls child records up into arrays.
--
-- Pattern used throughout:
--   1. CREATE STREAM over the Avro CDC topic, declaring only the columns needed.
--   2. CREATE TABLE ... AS SELECT key, LATEST_BY_OFFSET(col) ... GROUP BY key
--      EMIT CHANGES, which keeps the most recent value seen per key.
--   3. HAVING LATEST_BY_OFFSET(OP_TYPE) != 'D' so a deleted key disappears from
--      the materialised table instead of lingering with stale values. That is
--      the tombstone filter.
--
-- LATEST_BY_OFFSET(col, false) keeps the latest value but ignores nulls, so a
-- partial CDC payload does not erase a column that was previously populated.
--
-- The environment is a ksqlDB variable so the same file serves every
-- environment. Change the DEFINE below, or render the file from your deployment
-- tooling, before submitting it:
--   ksql http://ksqldb:8088 -f orders_latest_state.sql

DEFINE env = 'dev';

SET 'auto.offset.reset' = 'earliest';

-- ---------------------------------------------------------------------------
-- Orders
-- ---------------------------------------------------------------------------

CREATE STREAM str_orders (
    order_id     VARCHAR,
    customer_ref VARCHAR,
    channel_id   VARCHAR,
    status       VARCHAR,
    event_date   VARCHAR,
    link_origin  VARCHAR,
    op_type      VARCHAR
) WITH (
    KAFKA_TOPIC  = 'cdc.${env}.orders',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE tab_orders WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_orders',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    o.order_id                                  order_id,
    LATEST_BY_OFFSET(o.customer_ref, false)     customer_ref,
    LATEST_BY_OFFSET(o.channel_id, false)       channel_id,
    LATEST_BY_OFFSET(o.status, false)           status,
    LATEST_BY_OFFSET(o.event_date, false)       event_date,
    LATEST_BY_OFFSET(o.link_origin, false)      link_origin,
    LATEST_BY_OFFSET(o.op_type)                 op_type
FROM str_orders o
GROUP BY o.order_id
HAVING LATEST_BY_OFFSET(o.op_type) != 'D'
EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Order lines: a child table keyed by (order_id, line_number)
-- ---------------------------------------------------------------------------

CREATE STREAM str_order_lines (
    order_id    VARCHAR,
    line_number INTEGER,
    product_ref VARCHAR,
    quantity    INTEGER,
    unit_price  DECIMAL(12, 2),
    op_type     VARCHAR
) WITH (
    KAFKA_TOPIC  = 'cdc.${env}.order_lines',
    VALUE_FORMAT = 'AVRO'
);

-- A composite primary key is expressed as a STRUCT, which lets a single column
-- carry the whole key and keeps downstream joins on one equality.
CREATE TABLE tab_order_lines WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_order_lines',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    STRUCT(order_id := l.order_id, line_number := l.line_number) order_line_key,
    LATEST_BY_OFFSET(l.product_ref, false)  product_ref,
    LATEST_BY_OFFSET(l.quantity, false)     quantity,
    LATEST_BY_OFFSET(l.unit_price, false)   unit_price
FROM str_order_lines l
GROUP BY STRUCT(order_id := l.order_id, line_number := l.line_number)
HAVING LATEST_BY_OFFSET(l.op_type) != 'D'
EMIT CHANGES;

-- Roll the lines of an order up into one array so the enriched view below can
-- serve a whole order in a single record.
CREATE TABLE tab_order_lines_by_order WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_order_lines_by_order',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    l.order_id order_id,
    COLLECT_LIST(STRUCT(
        line_number := l.line_number,
        product_ref := l.product_ref,
        quantity    := l.quantity,
        unit_price  := l.unit_price
    )) lines,
    SUM(l.quantity * l.unit_price) order_total
FROM str_order_lines l
GROUP BY l.order_id
HAVING LATEST_BY_OFFSET(l.op_type) != 'D'
EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Customers
-- ---------------------------------------------------------------------------

CREATE STREAM str_customers (
    customer_ref VARCHAR,
    customer_name VARCHAR,
    country_code VARCHAR,
    segment      VARCHAR,
    op_type      VARCHAR
) WITH (
    KAFKA_TOPIC  = 'cdc.${env}.customers',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE tab_customers WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_customers',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    c.customer_ref                            customer_ref,
    LATEST_BY_OFFSET(c.customer_name, false)  customer_name,
    LATEST_BY_OFFSET(c.country_code, false)   country_code,
    LATEST_BY_OFFSET(c.segment, false)        segment
FROM str_customers c
GROUP BY c.customer_ref
HAVING LATEST_BY_OFFSET(c.op_type) != 'D'
EMIT CHANGES;

-- Distinct set of customer references ever seen on an order, one row per order.
CREATE TABLE tab_order_customer_refs WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_order_customer_refs',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    o.order_id order_id,
    COLLECT_SET(TRIM(o.customer_ref)) customer_refs
FROM str_orders o
GROUP BY o.order_id
HAVING LATEST_BY_OFFSET(o.op_type) != 'D'
EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Enriched latest-state view
--
-- Table-table joins are used, not stream-table joins, so the result is itself a
-- materialised table that re-emits whenever either side changes. AS_VALUE keeps
-- the key available as a normal column for consumers that ignore record keys.
-- ---------------------------------------------------------------------------

CREATE TABLE tab_orders_enriched WITH (
    KAFKA_TOPIC = 'ksql.${env}.tab_orders_enriched',
    PARTITIONS  = 3,
    REPLICAS    = 1,
    FORMAT      = 'AVRO'
) AS SELECT
    o.order_id            order_key,
    AS_VALUE(o.order_id)  order_id,
    o.status              status,
    o.event_date          event_date,
    o.channel_id          channel_id,
    o.link_origin         link_origin,
    o.customer_ref        customer_ref,
    r.customer_refs       customer_refs,
    c.customer_name       customer_name,
    c.country_code        country_code,
    c.segment             customer_segment,
    l.lines               lines,
    l.order_total         order_total
FROM tab_orders o
LEFT OUTER JOIN tab_customers c           ON o.customer_ref = c.customer_ref
LEFT OUTER JOIN tab_order_lines_by_order l ON o.order_id = l.order_id
LEFT OUTER JOIN tab_order_customer_refs r  ON o.order_id = r.order_id
EMIT CHANGES;
