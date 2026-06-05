# Replication & Ingestion Architectures: On-Prem SQL Server → Lakehouse (Fabric / Databricks)

**Context:** 14 active MFC sites, each running NEXiA pharmacy automation on SQL Server 2019. A large number of tables use CDC to feed the analytics plane — primarily Microsoft Fabric, with Databricks (Lakeflow) also in use. Sources are busy OLTP systems; CDC log maintenance is a significant burden, and ingestion outages have forced sites to **stop CDC entirely**, the worst-case operational outcome.

**Goal of this document:** catalog the realistic replication/ingestion architecture patterns for this environment, explain their source-impact trade-offs, and frame a path to a more resilient design.

---

## 1. Why the Current Architecture Hurts

Understanding the failure mode is essential to evaluating alternatives.

### 1.1 CDC mechanics on the source

SQL Server CDC reads the **transaction log** via a capture job (Log Reader–based) and materializes changes into `cdc.<capture_instance>_CT` tables inside the *source database*. Three costs follow:

1. **Log retention pressure.** The log cannot truncate past the point the capture job has scanned. If the capture job falls behind on a busy system, the log grows.
2. **Change-table storage and cleanup.** The cleanup job deletes rows past the retention window (default 3 days). With many captured tables on a busy system, both the capture *writes* and the cleanup *deletes* are real I/O on the OLTP box.
3. **Subscriber lag amplification.** When the downstream consumer (Lakeflow gateway, mirroring, etc.) breaks or stalls, change tables and/or retention must stretch to cover the outage. Past the window, the consumer needs a **full re-snapshot** — which hammers the source far harder than steady-state CDC. This is the cycle that has forced CDC to be switched off.

> **Key principle:** the source pain is rarely CDC itself — it's *unreliable consumers* combined with retention windows. Any target architecture must make consumer outages cheap for the source to absorb.

### 1.2 The 14-site multiplier

Every pattern below must be deployed and operated ×14. Per-site bespoke pipelines are the second failure mode here: prefer patterns that are templated, centrally monitored, and tolerant of per-site schema/version drift (NEXiA upgrades won't be simultaneous).

---

## 2. Change-Capture Mechanisms (source-side options)

Before the pipeline patterns, the menu of capture mechanisms — because several pipeline patterns can ride on more than one of these:

| Mechanism | How | Source cost | History | Deletes | Notes |
|---|---|---|---|---|---|
| **CDC** | Log-scan → change tables | High (capture job, CT-table I/O, cleanup, log retention) | Full intermediate history, before/after images | Yes | Current approach; richest fidelity |
| **Change Tracking (CT)** | Synchronous, inline with DML; stores PKs + version only | Low–moderate (small inline write cost, autocleanup) | Net change only (no intermediate values) | Yes (PK only) | Consumer must join back to base table to get current row state |
| **rowversion / datetime2 cursor columns** | Query `WHERE cursor > @watermark` | Query-time scan cost only (index the cursor column) | None | **No** (hard deletes invisible) | What Lakeflow's query-based connector uses; soft-delete or periodic reconciliation needed |
| **Transactional replication / AG readable secondary** | Log reader → distributor / redo on secondary | Log-reader cost similar to CDC, but consumer decoupled | Full | Yes | Used to *offload* analytics reads, not to feed lakehouse directly |
| **Snapshot/batch export** | Full or partitioned extracts | Heavy but schedulable (off-peak) | None | Via full compare | Fallback / backfill path |

**CT vs CDC is the pivotal choice for busy NEXiA sources.** CT eliminates the capture/cleanup job machinery and log-retention coupling; the trade is losing intermediate history and needing well-behaved consumer queries (snapshot isolation recommended to avoid CT cleanup races). If the analytics plane only needs *current state* per table (typical for reporting), CT or cursor columns are materially cheaper for the source than CDC.

---

## 3. Pipeline Architecture Patterns

### Pattern A — Lakeflow Connect managed SQL Server connector (current Databricks path)

Gateway on **classic compute** connects to the source (static egress IP via NAT → firewall rule), stages changes to a Unity Catalog volume, and a serverless ingestion pipeline applies them to Delta tables. Supports **CDC and Change Tracking** as the capture mechanism.

- **Pros:** managed, low-code, gap-free CDC/CT consumption, SCD handling, gateway buffers consumer hiccups.
- **Cons:** still a per-source CDC/CT consumer (source burden remains); gateway is per-source-network infrastructure ×14 sites; Databricks-only destination.
- **Fit:** good where Databricks is the destination and CT (not CDC) can be the capture mechanism to lighten sources.

### Pattern B — Lakeflow query-based connector (the POC in flight)

No CDC required: incremental pulls driven by **cursor columns** per table. Runs on serverless by default (the on-prem firewall/dynamic-IP problem discussed in the meeting); classic compute support via API is in beta → public preview.

- **Pros:** removes CDC machinery from the source entirely; simple mental model; cheap for append-mostly tables with reliable monotonic columns.
- **Cons:** no hard-delete capture; needs an indexed cursor column on every table (vendor schema — NEXiA — may not cooperate); query load lands on the OLTP box at poll time; networking story (serverless egress) still maturing; no DAB/IaC support yet.
- **Fit:** selective use for high-churn, append-heavy tables that don't need delete fidelity. Not a wholesale CDC replacement for a vendor OLTP schema.

### Pattern C — Fabric Database Mirroring (primary Fabric path)

Fabric Mirroring supports **on-prem SQL Server (2016–2022, incl. 2019) using CDC under the hood**, replicating into OneLake delta tables via an **on-premises data gateway / VNet data gateway**. SQL Server 2025 introduces a lighter-weight change-feed mechanism, but 2019 mirroring **rides on CDC** — so it does *not* remove the source burden; it's another CDC consumer.

- **Pros:** native, near-real-time, free mirroring storage, data lands directly in the primary analytics plane; OneLake shortcuts make mirrored data consumable elsewhere.
- **Cons:** CDC-based for SQL 2019 (same source costs and same outage/retention failure mode); per-database gateway configuration ×14; limitations on some table/column types.
- **Fit:** strong candidate as the **single** CDC consumer per site *if* Fabric is confirmed as the one landing zone (see §5). Upgrading sources to SQL Server 2025 later would materially lighten this path.

### Pattern D — Open Mirroring / third-party log-based replication into OneLake

Fabric **Open Mirroring** lets any agent write change data into a mirrored database landing zone. Purpose-built partners (e.g., Striim SQL2Fabric, Qlik, Fivetran/HVR) do **log-based capture** with their own agent reading the transaction log, typically *without* requiring SQL Server CDC change tables.

- **Pros:** log-based capture with minimal source-side machinery (no CT tables/cleanup jobs); mature retry/buffering semantics — consumer outages are absorbed by the tool, not the source's log retention; multi-destination fan-out (can feed Fabric *and* Databricks/ADLS from one capture).
- **Cons:** license cost; another vendor in the stack; agent deployment per site; security review.
- **Fit:** strongest pattern when **source protection is the top priority** and budget allows. One capture per site, replayable to N destinations.

### Pattern E — Streaming backbone (Debezium + Kafka / Azure Event Hubs)

Debezium SQL Server connector (uses CDC) → Event Hubs/Kafka → consumed by both Fabric (Eventstreams/Real-Time Intelligence) and Databricks (Structured Streaming / Lakeflow Declarative Pipelines).

- **Pros:** true decoupling — the bus absorbs consumer outages; one capture, many consumers; event-time replay; org-standard pattern at scale.
- **Cons:** Debezium still rides on SQL Server CDC (source burden remains); significant platform engineering to run ×14 sites; schema-evolution discipline required.
- **Fit:** right answer if the org is heading toward real-time/event-driven use cases broadly; heavy if the only goal is analytics replication.

### Pattern F — Offload tier: replicate first, capture second

Use **transactional replication or an Always On readable secondary** to maintain a near-real-time copy of each site DB on a *central or per-site staging SQL Server*, then run CDC/CT/extracts against the **replica**, never the OLTP primary.

- **Pros:** the OLTP box does only log-reader work (cheap, well-understood, decades-mature); all analytics-driven load (CDC change tables, snapshot reloads, ad-hoc backfills) moves to hardware you control; consumer outages stop threatening the sources entirely.
- **Cons:** more SQL Server infrastructure to own (licensing, HA, patching); replication has its own ops surface; one more hop of latency; CDC on an AG secondary isn't supported (capture runs on primary) — so the pattern is replication→staging-primary, with CDC enabled on the staging copy.
- **Fit:** the classic enterprise answer to exactly this problem ("busy source + fragile downstream"). Especially attractive for vendor databases (NEXiA) you can't modify: the staging tier is yours to abuse.

### Pattern G — Watermark batch extracts (ADF / Fabric Data Factory pipelines)

Scheduled incremental copies using rowversion/modified-date watermarks, landing parquet/Delta in OneLake or ADLS.

- **Pros:** simplest, cheapest, no source machinery, fully schedulable off-peak.
- **Cons:** latency in hours, no deletes, per-table configuration; query load at extract time.
- **Fit:** the right tool for slowly-changing reference/dimension tables — which are often a large fraction of "a significant number of tables using CDC" that never needed CDC in the first place.

---

## 4. Cross-Cutting Concerns

**Networking / egress identity.** Every Databricks pattern reduces to "what stable identity reaches the on-prem firewall": classic compute + NAT static IP (works today), or Private Link (cleaner, removes the public path). Fabric patterns use the **on-premises data gateway**, an outbound-only agent — generally an easier security conversation than inbound firewall rules. Standardize one answer per platform and template it ×14.

**Tiering the table inventory.** Audit the CDC-enabled table list per site and classify: (1) needs real-time + delete fidelity → log-based capture; (2) append-heavy, current-state-only → CT/cursor-based; (3) slowly changing → batch watermark. Shrinking the CDC footprint is the cheapest performance win available, no architecture change required.

**Consumer-outage resilience (the actual root cause).** Whatever the pattern, the design target is: *a 72-hour consumer outage must not threaten the source*. Levers: capture into a buffer the source doesn't own (Pattern D/E/F), generous-but-bounded CDC retention with monitored headroom, automated re-snapshot runbooks that are partitioned/throttled, and alerting on capture-job latency (`sys.dm_cdc_log_scan_sessions`), change-table growth, and log `log_reuse_wait_desc`.

**CDC tuning if CDC stays (interim hardening).** Capture job parameters (`maxtrans`, `maxscans`, `pollinginterval`) tuned for sustained throughput; cleanup scheduled off-peak with bounded batch sizes; retention sized to realistic outage windows; capture instances pruned to required columns; monitor and alert *before* retention breach forces a reload.

**Schema drift across 14 sites.** NEXiA upgrades will roll site by site. The ingestion layer must tolerate additive drift (auto-map new columns) and quarantine breaking drift per site without halting the other 13. This favors managed connectors / third-party tools with built-in drift handling over hand-rolled pipelines.

**Site topology.** Prefer a hub model: identical templated capture per site → site-partitioned landing (`/site_id=NN/`) → centralized merge into conformed silver tables. Keep per-site credentials/gateways isolated so one site's failure or maintenance window doesn't block the fleet.

---

## 5. The Dual-Platform Question (Fabric *and* Databricks)

This is the most consequential architectural decision on the table. **Running independent CDC/CT consumers from both Fabric and Databricks against the same busy NEXiA sources doubles the source burden** — the precise problem under investigation.

Target principle: **capture once per site, share downstream.**

Realistic shapes:

1. **Fabric-primary:** mirror (Pattern C/D) into OneLake → Databricks reads the Delta tables (OneLake is ADLS-compatible; Delta is the common format). Databricks workloads consume from OneLake/ADLS, never from the sources.
2. **Databricks-primary:** Lakeflow Connect (Pattern A, CT-based) → Delta in ADLS/Unity Catalog → Fabric consumes via **OneLake shortcuts** to the Delta tables. Fabric never touches the sources.
3. **Neutral bus:** Pattern D or E lands changes once; both platforms subscribe. Highest decoupling, highest build cost.

Any of the three is defensible; running two capture stacks side-by-side indefinitely is not. The meeting's Databricks POC and the Fabric landing-zone strategy need to be reconciled explicitly.

---

## 6. Decision Matrix

| Pattern | Source burden | Latency | Deletes | Ops effort ×14 | Multi-destination | Maturity for SQL 2019 on-prem |
|---|---|---|---|---|---|---|
| A. Lakeflow managed (CDC/CT) | Med (CT) / High (CDC) | Minutes | Yes | Medium | No (Databricks) | GA |
| B. Lakeflow query-based | Low–Med (query load) | Minutes–hours | **No** | Medium | No (Databricks) | Preview/beta edges |
| C. Fabric Mirroring | High (CDC-based on 2019) | Near-real-time | Yes | Medium | Via shortcuts | GA |
| D. Open Mirroring / 3rd-party log-based | **Low** | Seconds–minutes | Yes | Medium (vendor-assisted) | **Yes** | GA (vendor) |
| E. Debezium + event bus | High (CDC-based) | Seconds | Yes | **High** | **Yes** | Mature OSS |
| F. Offload replica tier | **Lowest on OLTP** | Minutes (+hop) | Yes | High (infra) | Yes (capture from replica) | Very mature |
| G. Watermark batch | Low | Hours | No | Low | Yes | Mature |

---

## 7. Recommended Direction (phased)

1. **Now — stabilize:** tune and monitor existing CDC (retention headroom alerts, capture-job latency, cleanup scheduling); build the throttled re-snapshot runbook so an outage never again forces CDC off.
2. **Now — shrink:** tier the table inventory; move reference/slowly-changing tables off CDC to watermark batch (Pattern G). Expect a large reduction in change-table churn for free.
3. **Next — decide the single-capture strategy:** pick Fabric-primary, Databricks-primary, or neutral-bus (§5). This decision gates everything else and should be made before the Lakeflow query-based POC hardens into a parallel production path.
4. **Next — pilot the source-protecting capture:** for one busy site, pilot either CT-based Lakeflow (A), third-party log-based → Open Mirroring (D), or an offload replica (F), and measure source impact vs. today's CDC.
5. **Later — consider SQL Server 2025 upgrade path:** its native change feed for Fabric mirroring removes the CDC change-table burden entirely and may obsolete much of the middleware.

---

## Sources

- [Lakeflow Connect: SQL Server connector (Databricks blog)](https://www.databricks.com/blog/lakeflow-connect-efficient-and-easy-data-ingestion-using-sql-server-connector)
- [Managed connectors in Lakeflow Connect (Databricks docs)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [Query-based connectors (Databricks docs)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview)
- [Ingest data from SQL Server (Databricks docs)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline)
- [Configure SQL Server for ingestion into Databricks](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-source-setup)
- [Fabric Mirrored Databases from SQL Server (Microsoft Learn)](https://learn.microsoft.com/en-us/fabric/mirroring/sql-server)
- [Fabric Mirroring overview (Microsoft Learn)](https://learn.microsoft.com/en-us/fabric/mirroring/overview)
- [Limitations: Fabric mirrored databases from SQL Server](https://learn.microsoft.com/en-us/fabric/mirroring/sql-server-limitations)
- [Mirroring for SQL Server in Fabric — GA announcement](https://blog.fabric.microsoft.com/en-GB/blog/mirroring-for-sql-server-in-microsoft-fabric-generally-available/)
- [Striim SQL2Fabric mirroring](https://www.striim.com/blog/mirroring-sql-server-database-microsoft-fabric/)
