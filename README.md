# Distributed Compute Platform

Go-based distributed job execution with **PostgreSQL, Redis Streams, REST, WebSockets, Docker, and Kubernetes**. The repository includes five synthetic workloads, concurrent worker pools, a transactional outbox, idempotent submission, exponential retries, lease-based crash recovery, a 10,000-job load generator, tests, and CI.

> **Performance disclosure:** 450+ jobs/sec and a 99.9% completion rate are the best results I was able to achieve. Run the included benchmark on your own deployment before claiming them for this implementation.

## Architecture

```text
                        POST /v1/jobs         GET /v1/jobs/{id}
                              |                     ^
                         +---------+                |
                         | Go API  |------ WebSocket updates
                         +----+----+                ^
                              |                     | Redis Pub/Sub + DB polling
                              v                     |
                        +------------+              |
                        | PostgreSQL | <------------+---------+
                        | jobs       |                         |
                        | outbox     | <- atomic status updates
                        +------+-----+                         |
                               |                               |
                    +----------v---------+                     |
                    | Go dispatcher(s)   |                     |
                    +----------+---------+                     |
                               | XADD                          |
                               v                               |
                        +--------------+                       |
                        | Redis Stream |                       |
                        | Consumer grp |                       |
                        +------+-------+                       |
                               | XREADGROUP / XAUTOCLAIM       |
                      +--------+---------+                     |
                      | Go worker pools  | --------------------+
                      | retries / leases |
                      +------------------+
```

**Reliability design:** API insertion and an outbox event happen in one PostgreSQL transaction. Dispatchers claim due outbox events and publish job IDs to a Redis Stream. Workers atomically claim a job in PostgreSQL; duplicate queue deliveries cannot execute the same job concurrently while its lease is active. Success and failure update durable state. Failed jobs enqueue a delayed retry with exponential backoff (up to `max_attempts`). Redis message acknowledgments occur only after durable outcome writes. `XAUTOCLAIM` recovers unacknowledged deliveries after worker crashes, and a dispatcher sweep closes expired final attempts. Pub/Sub accelerates WebSocket updates; the DB remains authoritative.

This is **at-least-once delivery**, not mathematically exactly-once execution. Built-in workloads are deterministic/idempotent or safe to repeat; do not add externally side-effecting jobs without your own idempotency protections.

## Quick start (Docker Compose)

Requirements: Docker Engine with the Compose plugin.

```bash
# From repository root
cp .env.example .env
make up
curl -fsS http://localhost:8080/health/ready
make smoke
```

`make up` starts PostgreSQL 16, Redis 7 with append-only persistence, a one-shot database migration, the Go API, the dispatcher, and **3 worker containers**. Each worker uses 16 concurrent job goroutines by default.

```bash
make logs                 # tail API/dispatcher/worker logs
make scale                # increase to 5 worker containers
make down                 # stop but keep volumes
make reset                # remove containers and persistent volumes
```

## REST API

| Method | Route | Action |
|---|---|---|
| `POST` | `/v1/jobs` | Submit a job (202 Accepted) |
| `GET` | `/v1/jobs/{id}` | Read status, attempts, and result |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel a queued job or request running cancellation |
| `GET` | `/v1/jobs?limit=50` | Latest jobs, maximum 500 |
| `GET` | `/v1/batches/{batch_id}/stats` | Aggregate status counters for a batch |
| `GET` | `/v1/ws?job_id={id}` | WebSocket snapshots/status changes |
| `GET` | `/health/live` | Liveness |
| `GET` | `/health/ready` | PostgreSQL and Redis connectivity |

Example:

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"text","payload":{"text":"hello distributed systems"},"max_attempts":3,"idempotency_key":"demo-001","batch_id":"demo"}'
# Copy the id and run:
curl http://localhost:8080/v1/jobs/REPLACE_WITH_JOB_ID
```

WebSocket example (JavaScript; same-origin browser connections are allowed by default):

```javascript
const ws = new WebSocket('ws://localhost:8080/v1/ws?job_id=REPLACE_WITH_JOB_ID');
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

### Available job types

The load generator creates the following deterministic payloads; no downloaded dataset is needed.

| Workload | Share of benchmark | Example `payload` |
|---|---:|---|
| `json_transform` | 35% | `{"object":{"a":1},"prefix":"out_"}` |
| `text` | 25% | `{"text":"a quick example"}` |
| `numeric` | 20% | `{"n":1000}` |
| `aggregate` | 10% | `{"values":[1,2,3,4]}` |
| `simulated_io` | 10% | `{"delay_ms":5}` |

Inputs are limited to a 1 MiB request, a maximum 5-second simulated I/O delay, and bounded compute sizes. `max_attempts` defaults to 3 (range 1–10). `idempotency_key` deduplicates submissions; `batch_id` enables aggregate metrics. For an intentional retry test, submit `{"type":"numeric","payload":{"n":-1}}` and watch it fail after three attempts.

## Testing and benchmarking

```bash
# Requires Go 1.23+ and access to download Go modules
make test
make build

# Requires running Docker Compose services
make smoke
cd src && INTEGRATION_URL=http://localhost:8080 go test -v ./integration

# 10,000 mixed jobs; reports submission rate, effective throughput, failure count,
# and completion percentage for this specific run
make bench
# Or customize:
cd src && go run ./cmd/bench -url http://localhost:8080 -n 10000 -concurrency 64 -timeout 5m
```

**Benchmark interpretation:** `effective_jobs_per_sec = succeeded / (last observation time - first submission time)`, so it includes queueing and client submission. `completion_rate = succeeded / HTTP-accepted jobs`, not jobs merely published to Redis. Results vary with hardware, DB/Redis resources, number of replicas, and workload. No throughput is hard-coded. `99.9%` requires at least 9,990 successes out of 10,000 accepted jobs.

The CI pipeline runs Go unit tests with the race detector, static analysis, Docker Compose, a smoke test, and integration tests that check idempotency, terminal retry failure, WebSocket snapshots, and cancellation.

## Kubernetes

Requires a Kubernetes cluster with a default StorageClass, a working container registry or a local-image workflow, and a metrics-server for CPU-based HPA scaling. **Change the development passwords and connection strings** in `k8s/01-config.yaml` before external deployment.

Build the per-service images (from the repository root):

```bash
for service in api worker dispatcher migrate; do
  docker build -f docker/Dockerfile --build-arg SERVICE="$service" \
    -t "compute-$service:local" .
done
```

For **kind**, run `kind load docker-image compute-api:local compute-worker:local compute-dispatcher:local compute-migrate:local`; for **minikube**, use `minikube image load IMAGE` for each of the four images. For a remote cluster, push images to a registry and update the `image` fields in the manifests.

Apply dependencies and wait for the schema before deploying the app:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-config.yaml
kubectl apply -f k8s/02-dependencies.yaml
kubectl apply -f k8s/03-migrate.yaml
kubectl wait -n compute --for=condition=complete job/compute-migrate --timeout=300s
kubectl apply -f k8s/04-application.yaml
kubectl port-forward -n compute svc/compute-api 8080:8080
```

Worker count defaults to 3 and HPA can scale it to 10 replicas based on CPU. Redis and PostgreSQL run as single-replica StatefulSets **for local/demo use**; production requires managed or replicated services, TLS, secret management, access controls, monitoring, backups, and capacity testing. Restart and rolling upgrades are provided by Kubernetes Deployments.

## Repository layout

```text
distributed-compute-platform/
├── src/
│   ├── cmd/{api,worker,dispatcher,migrate,bench}/
│   ├── internal/
│   │   ├── config/       # environment + clients
│   │   ├── model/        # job schema + validation
│   │   ├── engine/       # five workloads
│   │   ├── store/        # PostgreSQL transactions and outbox
│   │   ├── queue/        # Redis Streams and notifications
│   │   ├── runner/       # concurrent worker pool, retries and leases
│   │   ├── events/       # outbox dispatcher
│   │   ├── httpapi/      # REST + WebSocket
│   │   └── migrations/   # schema embedded in executable
│   ├── integration/      # end-to-end tests (optional running service)
│   └── go.mod
├── docker/Dockerfile
├── docker-compose.yml
├── k8s/                   # namespace, secrets, stateful services, jobs, deployments, HPA
├── scripts/smoke.sh
├── .github/workflows/ci.yml
├── Makefile
├── .env.example
└── README.md
```

## Operational notes

- **Crash recovery:** pending Redis Stream messages are reclaimed after their minimum idle time has elapsed (lease + 5 seconds) and retried only when their DB lease has expired. Healthy jobs renew DB leases. Redis Stream persistence (`appendonly yes`) and PostgreSQL volumes survive container restarts in Compose. For larger replica counts, increase PostgreSQL capacity or tune `DB_MAX_CONNS` (12 per service instance by default).
- **Retry delay:** 0.5 seconds, 1 second, 2 seconds, etc., with a 16-second cap. Attempts are bounded at `max_attempts`.
- **Termination:** a running job only commits results if it still owns the database lease. Worker shutdown cancels in-flight jobs; interrupted deliveries stay unacknowledged and are reclaimed after the idle threshold. A 40-second Kubernetes termination grace period provides time for clean shutdown.
- **Cancellation:** queued jobs become `canceled` immediately. Running jobs set `cancel_requested`; the worker picks it up at heartbeat (or before success is committed). For immediate cancellation in long jobs, lower the heartbeat interval or implement direct control messaging.
- **Outbox backlog:** multiple dispatchers can run safely via `SKIP LOCKED`. Re-published messages are safe because PostgreSQL controls claiming and job state transitions.
- **Security:** no user authentication, API rate limiting, or TLS is implemented. The stack is a local systems project, not a public multi-tenant compute service.
