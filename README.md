# kafka-cdc-warehouse-sink

An end-to-end change-data-capture pipeline: a source database's row-level changes
are captured to Kafka as Avro with Schema Registry, optionally collapsed into
latest-state tables with ksqlDB, and consumed by a Python sink that batches the
records and applies each batch to an analytical warehouse **inside a single
transaction, committing the Kafka offsets only after the warehouse transaction
has committed**. That ordering is the whole point of the component: it turns the
failure mode from "silently lost batch" into "batch delivered twice", and the
six-step upsert it applies is written to absorb the duplicate. The result is
at-least-once delivery with no partial application, which is the strongest
guarantee available without a distributed transaction between Kafka and the
warehouse.

## Architecture

```
  ┌──────────────────┐
  │  source database │   operational OLTP tables
  └────────┬─────────┘
           │ row-level changes (insert / update / delete)
           ▼
  ┌──────────────────────────────┐
  │  Kafka Connect CDC connector │   deploy/helm/cdc-connect
  └────────┬─────────────────────┘
           │ Avro records, schemas registered per subject
           ▼
  ┌───────────────────┐        ┌──────────────────────┐
  │   Kafka  topics   │◄──────►│   Schema Registry    │
  │  cdc.{env}.orders │        │  cdc.{env}.orders-   │
  └────────┬──────────┘        │        value         │
           │                   └──────────────────────┘
           │
           ├────────────────────────────────┐
           │                                │ optional
           │                                ▼
           │                   ┌─────────────────────────┐
           │                   │  ksqlDB materialisation │  sql/ksql/
           │                   │  LATEST_BY_OFFSET,      │
           │                   │  tombstone filtering    │
           │                   └───────────┬─────────────┘
           │                               │ ksql.{env}.tab_* latest-state topics
           ▼                               ▼
  ┌────────────────────────────────────────────────────┐
  │              Python sink  (src/cdc_sink)           │
  │                                                    │
  │   poll ─► filter ─► buffer ─► 6-step upsert        │
  │                                  │                 │
  │                                  ▼                 │
  │                     warehouse COMMIT               │
  │                                  │                 │
  │                                  ▼                 │
  │                     Kafka offset COMMIT            │
  └───────────────────────┬────────────────────────────┘
                          │ one transaction per batch
                          ▼
  ┌──────────────────────────────────────┐
  │        analytical warehouse          │   sql/warehouse_schema.sql
  │  reference · current state · history │
  └──────────────────────────────────────┘
```

## Delivery semantics

The sink is **at-least-once with no partial application**. Three decisions
produce that guarantee.

**Auto-commit is off.** The consumer is built with
`enable.auto.commit=false`. The client never advances an offset on its own, so
offset progress means exactly one thing: the sink decided a batch was durable.

**The warehouse transaction commits first.** A batch is applied on one connection
with autocommit disabled. All six steps run, then the warehouse commits, and only
then are the offsets committed:

```python
with self.warehouse.cursor() as cursor:
    ...  # steps 1 to 6
self.warehouse.commit()             # durable in the warehouse
self.consumer.commit(asynchronous=False)   # only now acknowledged to Kafka
```

If the process dies between those two commits, the batch is redelivered on
restart and applied a second time. That is survivable: step 1 is guarded by `NOT
EXISTS`, step 4 clears before it inserts, and step 6 deletes before every insert,
so reapplying a batch converges on the same state rather than duplicating rows.

Reversing the order would break it. Committing offsets first means a crash before
the warehouse commit loses the batch with no trace, and nothing downstream can
detect the gap. Committing offsets last trades a possible duplicate for the
impossibility of silent loss, and the upsert is built to absorb the duplicate.

**The commit is synchronous.** `commit(asynchronous=False)` blocks until the
broker acknowledges. An asynchronous commit would return before the offset was
stored and reintroduce the window the ordering exists to close.

Two smaller details follow from the same reasoning:

- An event the filter discards still occupies an offset. It is acknowledged
  immediately, but **only while no batch is pending**. Committing a skipped
  record while records sit unapplied in the buffer would acknowledge data that is
  not in the warehouse yet.
- A partially filled batch is flushed when a poll returns nothing, so a quiet
  topic does not leave records waiting in memory for traffic that may not come.

Kafka's own ordering is preserved per partition, and the sink is single-threaded
per partition assignment, so a batch never interleaves with itself. Ordering
across partitions is not guaranteed, which is why the warehouse key must be the
Kafka message key. See Limitations.

## The six-step upsert

Each batch runs these six steps on one cursor, one transaction, in this order.
The full SQL is in [`src/cdc_sink/queries.py`](src/cdc_sink/queries.py); the
tables are defined in [`sql/warehouse_schema.sql`](sql/warehouse_schema.sql).

The batch itself is not written to a staging table. It is inlined as a derived
table built from `SELECT ... UNION ALL SELECT ...`, with **every value passed as
a bound parameter**. The statement text is constant for a given batch size; only
the parameter list changes. That avoids the catalog write a real temp table costs
per batch and removes any possibility of SQL injection from event payloads.

**Step 1 — backfill missing catalog customers.** A change event can reference a
customer no upstream load has delivered yet. Insert a placeholder row into
`customers_reference` first, so the joins in step 2 can stay inner and the event
is not dropped. The batch is grouped before insertion (a batch mentioning the
same new customer twice must insert one row) and the insert is guarded by `NOT
EXISTS`, which is what makes the step idempotent across retries.

**Step 2 — stage the batch against current state.** Create a session-scoped
`stage_order_events` table holding one row per event, already carrying the
warehouse surrogate keys of both sides of the link: `orders_reference` is joined
inner and restricted to operational sources, `customers_reference` is joined
outer so a brand new customer does not eliminate the row. Steps 3 to 6 read only
this table, so the expensive joins run once per batch instead of once per step.

**Step 3 — append candidate history.** Insert every new link proposal into
`order_customer_candidates_history`. Append-only: this is the audit trail, and it
keeps proposals that the current-state tables no longer show.

**Step 4 — refresh current candidates.** Delete the previous proposals for the
orders in this batch, then insert the new ones, so
`order_customer_candidates` reflects the latest proposal set instead of
accumulating rows.

**Step 5 — append confirmed-link history.** Insert into
`order_customer_links_history`, mapping `op_type` to an audit action. Unlike step
3 this covers deletes too, so the history reconstructs the full lifecycle of a
link rather than only its creations.

**Step 6 — replay operations onto the current-state table.** Read the staged rows
ordered by source transaction timestamp, with deletes sorted before inserts at an
identical timestamp, and apply them one at a time to `order_customer_links`.

This last step is deliberately row-at-a-time. A set-based statement cannot
express "apply these operations in this order" when one batch holds several
operations for the same key, and collapsing them first would lose the audit rows
steps 3 and 5 already wrote. The ordering matters because a CDC connector emits
an update as a delete followed by an insert: applying that pair backwards leaves
the wrong final state. Every insert is preceded by a delete for the same order,
which is what makes redelivery converge instead of duplicate.

## Features

- Avro consumption with Schema Registry, including basic-auth registries.
- Manual offset management with the commit ordering described above.
- Configurable batching with three flush triggers: batch size reached, idle poll
  with records pending, and clean shutdown.
- Relevance filter that discards malformed payloads, updates (delivered as
  delete/insert pairs) and link creations owned by another pipeline, without
  stalling offset progress.
- Recovery from a detached consumer by resubscribing rather than exiting.
- One long-lived warehouse connection, reused across batches, because these
  warehouses are slow to authenticate.
- Deterministic content-derived surrogate keys, so the reference backfill is
  idempotent.
- SQL restricted to the PostgreSQL / PostgreSQL-compatible-warehouse
  intersection: no `ON CONFLICT`, no `MERGE`, no vendor clock functions.
- Optional ksqlDB layer that collapses change streams into latest-state tables
  and filters tombstones.
- Kubernetes deployment where every credential arrives through `secretKeyRef`.
- A local docker-compose stack that runs the whole pipeline end to end.
- 98 offline tests. No test opens a socket.

## Prerequisites

| Requirement | Version | Needed for |
| --- | --- | --- |
| Python | 3.10 or newer | The sink and the sample producer |
| Docker with Compose v2 | recent | The local walkthrough |
| Kafka | 2.8 or newer | Any deployment |
| Schema Registry | any | Avro serialisation |
| PostgreSQL-compatible warehouse | any | The sink target |
| ksqlDB | 0.14 or newer | Optional materialisation layer (variable substitution) |
| kubectl / Helm 3 | recent | Kubernetes deployment |

## Configuration

Every setting is read from the environment. Nothing is hardcoded. Credentials
have no defaults, so a misconfigured deployment fails at startup rather than
connecting somewhere unintended.

### General

| Name | Description | Default |
| --- | --- | --- |
| `ENVIRONMENT` | Environment name, interpolated into the default topic name | `dev` |
| `LOG_LEVEL` | Root log level | `INFO` |
| `PYTHONUNBUFFERED` | Set to `1` so container logs are not buffered | unset |

### Kafka

| Name | Description | Default |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker list | `localhost:9092` |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` | `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | SASL mechanism, used only when the protocol is a SASL one | `PLAIN` |
| `KAFKA_SASL_USERNAME` | SASL user. **Secret** | unset |
| `KAFKA_SASL_PASSWORD` | SASL password. **Secret** | unset |
| `KAFKA_AUTO_OFFSET_RESET` | Where a new consumer group starts | `earliest` |
| `KAFKA_MAX_POLL_INTERVAL_MS` | Poll interval ceiling; must exceed the slowest batch | `900000` |
| `KAFKA_SESSION_TIMEOUT_MS` | Consumer session timeout | `600000` |
| `KAFKA_HEARTBEAT_INTERVAL_MS` | Heartbeat interval | `3000` |

### Schema Registry

| Name | Description | Default |
| --- | --- | --- |
| `SCHEMA_REGISTRY_URL` | Registry base URL | `http://localhost:8081` |
| `SCHEMA_REGISTRY_BASIC_AUTH` | Basic auth as `user:password`. Omitted from the client config when unset. **Secret** | unset |

### Topics

| Name | Description | Default |
| --- | --- | --- |
| `CDC_TOPIC` | Topic to consume | `cdc.${ENVIRONMENT}.orders` |
| `CONSUMER_GROUP` | Consumer group that carries the committed offsets | `cdc-warehouse-sink` |
| `SINK_OUTPUT_TOPIC` | Optional topic for republishing applied records. The producer is built only when this is set | unset |

### Batching

| Name | Description | Default |
| --- | --- | --- |
| `BATCH_SIZE` | Records buffered before a flush is triggered | `500` |
| `POLL_TIMEOUT_SECONDS` | Blocking poll timeout | `10` |
| `SLEEP_INTERVAL_SECONDS` | Pause after an idle poll | `5` |
| `CATALOG_SOURCE_ID` | `source_id` identifying the canonical customer catalog | `12` |
| `DEFAULT_LINK_SCORE` | Score written for a manually created link | `2` |

### Warehouse

| Name | Description | Default |
| --- | --- | --- |
| `WAREHOUSE_HOST` | Warehouse host | `localhost` |
| `WAREHOUSE_PORT` | Warehouse port | `5432` |
| `WAREHOUSE_DATABASE` | Database name | `warehouse` |
| `WAREHOUSE_SCHEMA` | Schema placed on the connection `search_path` | `public` |
| `WAREHOUSE_USER` | Login user. **Secret, required** | none, startup fails |
| `WAREHOUSE_PASSWORD` | Login password. **Secret, required** | none, startup fails |
| `WAREHOUSE_CONNECT_TIMEOUT` | Connection timeout in seconds | `10` |
| `WAREHOUSE_APPLICATION_NAME` | Reported to the warehouse for session attribution | `cdc-warehouse-sink` |

[`.env.example`](.env.example) lists all of them with placeholder values. Copy it
to `.env` for local work; `.env` is gitignored.

## Installation

```bash
git clone <repository-url>
cd kafka-cdc-warehouse-sink

python -m venv .venv
source .venv/bin/activate

# Runtime only
pip install -r requirements.txt
pip install -e .

# Or with the test and lint tooling
pip install -r requirements-dev.txt
pip install -e .
```

The container image is built from the same sources:

```bash
docker build -t cdc-warehouse-sink:1.0.0 .
```

It is a two-stage build. The wheel is produced in the first stage, so neither
build tooling nor the source tree reaches the runtime image, which runs as uid
`10001` with a read-only root filesystem.

## Usage

### Local walkthrough

The compose stack is Kafka in KRaft mode, Schema Registry, and a PostgreSQL
instance standing in for the analytical warehouse. It is plaintext and
single-node on purpose: it is a development harness, not a deployment template.

```bash
# 1. Infrastructure. The warehouse schema and 20 seed orders are applied on first
#    start from sql/warehouse_schema.sql.
docker compose up -d kafka schema-registry warehouse

# 2. Produce synthetic Avro CDC events. Registers the schema from
#    schemas/order_cdc_event.avsc on first run.
docker compose run --rm producer

# 3. Run the sink. It consumes, batches 10 at a time, and applies each batch.
docker compose up sink
```

The log shows the ordering the component exists for, once per batch:

```
Flushing 10 records
Step 1: backfill missing catalog customers
Step 2: stage the batch against current state
Step 3: append candidate history
Step 4: refresh current candidates
Step 5: append confirmed link history
Step 6: replay operations onto current links
Committing warehouse transaction
Committing Kafka offsets
```

Check the result:

```bash
docker compose exec warehouse psql -U warehouse -d warehouse -c "
  SELECT 'links' AS table_name, count(*) FROM order_customer_links
  UNION ALL SELECT 'links_history', count(*) FROM order_customer_links_history
  UNION ALL SELECT 'candidates', count(*) FROM order_customer_candidates
  UNION ALL SELECT 'customers', count(*) FROM customers_reference;"
```

`order_customer_links` holds at most one row per order (step 6 deletes before it
inserts), while the history tables keep every operation. Re-running the producer
and the sink leaves the link count unchanged and grows the history, which is the
idempotence claim in observable form.

The sample producer is tunable through `EVENT_COUNT`, `DELETE_RATIO`,
`AUTOMATIC_RATIO` and `RANDOM_SEED`. Roughly a quarter of the events it emits
carry `link_origin = 'A'`, which the sink discards, and some are emitted as
delete/insert pairs one millisecond apart, so step 6's ordering is actually
exercised.

The optional ksqlDB layer runs behind a profile:

```bash
docker compose --profile ksql up -d ksqldb

# The statements read cdc.{env}.order_lines and cdc.{env}.customers as well, so
# create those topics first; the sample producer only writes cdc.dev.orders.
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --partitions 3 --replication-factor 1 --topic cdc.dev.order_lines
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --partitions 3 --replication-factor 1 --topic cdc.dev.customers

docker compose exec ksqldb ksql http://localhost:8088 -f /opt/ksql/orders_latest_state.sql
```

The environment is a ksqlDB variable (`DEFINE env = 'dev';` at the top of the
file), so the same statements serve every environment.

Tear down with `docker compose down -v`.

### Kubernetes deployment

```bash
# 1. Create the Secret. Never from a committed file.
kubectl create namespace cdc-dev
kubectl create secret generic cdc-warehouse-sink-secrets \
  --namespace cdc-dev \
  --from-literal=kafka-sasl-username="$KAFKA_SASL_USERNAME" \
  --from-literal=kafka-sasl-password="$KAFKA_SASL_PASSWORD" \
  --from-literal=schema-registry-basic-auth="$SCHEMA_REGISTRY_BASIC_AUTH" \
  --from-literal=warehouse-user="$WAREHOUSE_USER" \
  --from-literal=warehouse-password="$WAREHOUSE_PASSWORD"

# 2. Review what will be applied, then apply it.
kubectl kustomize deploy/kubernetes/overlays/dev
kubectl apply -k deploy/kubernetes/overlays/dev
```

The source connector is a separate concern, deployed with the Helm chart in
`deploy/helm/cdc-connect`:

```bash
helm dependency update deploy/helm/cdc-connect
helm upgrade --install cdc deploy/helm/cdc-connect \
  --namespace cdc-dev \
  -f my-values.yaml          # your own file, kept out of the repository
```

## Kubernetes and secret handling

This is the part of the original design that most needed fixing, so it is worth
being explicit about.

**The problem.** The deployment this repository is derived from had one manifest
per environment, each embedding every Kafka, Schema Registry and warehouse
credential as a plaintext `env` value. Two near-identical files, every secret
readable by anyone with repository access or `kubectl get deployment -o yaml`,
rotation meaning an edit and a redeploy, and credentials leaking into CI logs and
review tooling.

**The fix.** One parameterised base manifest, and credentials that never appear
in it:

- [`base/deployment.yaml`](deploy/kubernetes/base/deployment.yaml) is a single
  manifest used by every environment. Non-sensitive configuration arrives through
  `envFrom.configMapRef`; every sensitive value arrives through an individual
  `valueFrom.secretKeyRef`. There is no credential in the file, so it is safe to
  commit and safe to render into a pull request.
- [`base/secret.example.yaml`](deploy/kubernetes/base/secret.example.yaml) holds
  placeholders only and is deliberately **not** a kustomize resource. It
  documents the five key names the Deployment expects, which is the contract an
  external secrets controller has to satisfy. The real Secret is created out of
  band.
- The [`dev`](deploy/kubernetes/overlays/dev) and
  [`prod`](deploy/kubernetes/overlays/prod) overlays differ only in namespace,
  replica count and non-sensitive ConfigMap literals.
- The Helm chart follows the same rule: Kafka and Schema Registry authentication
  is a `secretRef`, source-database credentials are mounted and read by the
  connector through `${file:...}` indirections, so no credential is templated
  into the `Connector` resource or written to the Connect config topic in
  cleartext.
- CI enforces it. The `validate-manifests` job greps the manifests for
  credential patterns and fails the build on a match, so the fix cannot quietly
  regress.

The pod also runs as non-root with a read-only root filesystem and all
capabilities dropped, and uses the `Recreate` strategy: two pods of the same
consumer group briefly overlapping would trigger a rebalance in the middle of a
warehouse transaction.

## Project structure

```
kafka-cdc-warehouse-sink/
├── .env.example                      Every environment variable, placeholders only
├── .github/workflows/ci.yml          Lint, tests on 3.10-3.12, manifest validation, image build
├── deploy/
│   ├── helm/cdc-connect/             Connect cluster + CDC source connector
│   │   ├── Chart.yaml                Declares the upstream Confluent chart as a dependency
│   │   ├── values.yaml               Placeholders only, no credentials
│   │   └── templates/
│   │       ├── _helpers.tpl
│   │       ├── connect.yaml          Connect CR, plugin fetched on demand
│   │       └── source-connector.yaml Connector CR, config from values
│   └── kubernetes/
│       ├── base/
│       │   ├── configmap.yaml        Non-sensitive configuration only
│       │   ├── deployment.yaml       One manifest, every credential via secretKeyRef
│       │   ├── secret.example.yaml   Placeholder template, not a kustomize resource
│       │   └── kustomization.yaml
│       └── overlays/{dev,prod}/      Namespace, replicas, ConfigMap literals
├── schemas/order_cdc_event.avsc      Avro schema of the change event
├── scripts/produce_sample_events.py  Synthetic Avro CDC producer
├── sql/
│   ├── warehouse_schema.sql          Reference, current-state and history tables
│   └── ksql/orders_latest_state.sql  Optional latest-state materialisation
├── src/cdc_sink/
│   ├── __main__.py                   Entry point, wires configuration to clients
│   ├── config.py                     Environment-driven settings, fail fast
│   ├── kafka_manager.py              Consumer and producer wrappers
│   ├── warehouse.py                  Long-lived connection, transaction boundary
│   ├── queries.py                    The six upsert steps
│   ├── hashing.py                    Deterministic surrogate keys
│   └── sink.py                       Poll, filter, buffer, flush, commit ordering
├── tests/
│   ├── conftest.py                   Offline fakes for Kafka and the warehouse
│   ├── test_commit_ordering.py       The core guarantee
│   ├── test_flush_steps.py           All six steps, in order, one transaction
│   ├── test_batching.py              Filtering, flush triggers, recovery
│   ├── test_kafka_manager.py         Client configuration
│   ├── test_queries.py               SQL shape and parameter binding
│   └── test_config.py                Defaults and validation
├── docker-compose.yml                Kafka, Schema Registry, PostgreSQL, sink, ksqlDB
├── Dockerfile                        Two-stage, non-root runtime
├── pyproject.toml                    Packaging, pytest and ruff configuration
├── requirements.txt                  Pinned runtime dependencies
└── requirements-dev.txt              Pinned test and lint dependencies
```

## Testing

```bash
pip install -r requirements-dev.txt
pip install -e .

python -m pytest
python -m pytest --cov=cdc_sink --cov-report=term-missing

ruff check src tests scripts
ruff format --check src tests scripts
```

98 tests, all offline. Kafka, Schema Registry and the warehouse are replaced by
in-process fakes in [`tests/conftest.py`](tests/conftest.py), so the suite never
opens a socket and is safe to run anywhere.

What is covered:

- **Commit ordering** ([`test_commit_ordering.py`](tests/test_commit_ordering.py)).
  The consumer and the warehouse write into one shared, ordered recorder, so the
  assertion is on the relative position of the two commits rather than on
  something inferred. It also asserts that every statement runs before the
  warehouse commit, that the ordering holds pairwise across consecutive batches,
  that a failing step or a failing warehouse commit leaves the offsets untouched
  and the batch buffered, and that the offset commit is synchronous. A second,
  independent check repeats the ordering assertion through attached mocks.
- **The six steps** ([`test_flush_steps.py`](tests/test_flush_steps.py)). All six
  run, in the documented order, on one cursor, and no business value reaches the
  warehouse as statement text rather than as a bound parameter.
- **Batching and recovery** ([`test_batching.py`](tests/test_batching.py)).
  Timestamp normalisation, the relevance filter case by case, the batch-size
  trigger, the empty-poll flush of a partial batch both directly and inside the
  loop, the rule that a skipped event is acknowledged only while no batch is
  pending, and resubscribe-on-`RuntimeError` including the fact that the
  uncommitted batch is dropped so Kafka's replay is the single source of it.
- **Client configuration** ([`test_kafka_manager.py`](tests/test_kafka_manager.py)).
  `confluent_kafka` is patched out. Asserts auto-commit is off, that SASL keys
  appear only for SASL protocols, that basic auth is omitted when unset, that the
  producer is idempotent and refuses to exist without an output topic, and that
  consumer and producer cannot drift apart on authentication.
- **SQL** ([`test_queries.py`](tests/test_queries.py)). Placeholder and parameter
  counts, that values never appear in statement text, that step 1 groups and
  guards with `NOT EXISTS`, that step 2 keeps the customer join outer, that steps
  3 to 5 read only the staging table, and that step 6 emits a delete before every
  insert and replays a delete/insert pair in order.
- **Configuration** ([`test_config.py`](tests/test_config.py)). Defaults, empty
  values treated as unset, numeric validation, and mandatory credentials failing
  at startup.

## Limitations and notes

- **Duplicates are possible, loss is not.** That is the deliberate trade. A crash
  between the two commits reapplies the batch. The upsert converges, but a
  consumer reading the history tables will see the duplicated audit rows.
- **Partition key must match the warehouse key.** Ordering is guaranteed per
  partition only. If two changes to the same order land on different partitions,
  step 6 can apply them out of order. Produce with the order identifier as the
  message key.
- **Scale out only up to the partition count.** Beyond that, extra replicas idle.
  Each replica processes its assigned partitions independently, which is safe
  because the warehouse key space is partitioned along with the topic.
- **No liveness probe.** The sink exposes no HTTP endpoint, so the Deployment has
  no readiness or liveness probe. In a real cluster, add a probe backed by a
  lag or heartbeat metric rather than process liveness, which would not detect a
  consumer that is alive but stalled.
- **No metrics endpoint.** Observability is structured logs. The Kafka client
  statistics callback is wired but only logs at debug level; exporting it to
  Prometheus would be the obvious next step.
- **`WAREHOUSE_SCHEMA` sets `search_path` on the connection.** The SQL uses
  unqualified table names, so a wrong value fails loudly at step 1 rather than
  writing to the wrong schema.
- **Step 6 is row-at-a-time.** Correct, and the bottleneck for very large
  batches. It is ordered-replay work that cannot be made set-based without losing
  the ordering guarantee. Batch sizes in the hundreds are the intended range.
- **`WarehouseManager.reconnect` exists but the loop does not call it.** A dropped
  connection surfaces as a failed batch, the offsets stay put, and the container
  restarts. Automatic reconnection inside the loop would need care not to
  reconnect mid-transaction.
- **The ksqlDB layer is optional and not exercised by the tests.** It is a
  materialisation convenience, not part of the delivery guarantee. The statements
  are validated by running them, not by an automated test.
- **The compose stack is single-node and unauthenticated.** Replication factor 1,
  `PLAINTEXT` everywhere, and the warehouse credentials in
  `docker-compose.yml` are local-only development values that exist nowhere else.
  Never derive a deployment from that file.
- **Redshift compatibility is by construction, not by test.** The SQL avoids
  constructs Redshift lacks and the driver speaks the PostgreSQL wire protocol,
  but CI only exercises PostgreSQL. The one PostgreSQL-specific fragment is the
  `generate_series` seed block in `warehouse_schema.sql`, which is for the local
  walkthrough only.
- **The domain is generic on purpose.** Orders linked to customers is a stand-in.
  The engineering worth looking at is the commit ordering, the six-step structure
  and the secret handling, none of which depend on the domain.

## License

MIT. See [LICENSE](LICENSE).

Copyright (c) 2026 Ivan Fernandez García
