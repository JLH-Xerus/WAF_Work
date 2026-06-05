# Meeting Notes — Lakeflow Connect Ingestion: Compute & Connectivity Options for On-Prem SQL Server

**Source:** `LakeflowConversation.txt` (Teams transcript, ~33 min)
**Approx. date:** ~2 weeks before Databricks Data + AI Summit (referenced in-meeting), i.e., late May / early June 2026
**Transcript gaps:** ~0:53–8:47 and ~11:10–14:58 are missing (likely screen-share segments). Notes below reconstruct context where possible and flag inferences.

---

## Attendees

| Person | Role (inferred) |
|---|---|
| Stephen Engelhardt | Databricks platform owner (DLX environment); meeting facilitator |
| Venkata Reddy | Engineering lead on the ingestion POC team; WAF workspace experience |
| Kevin Nguyen | Databricks (vendor) — solutions/field engineer; liaison to Lakeflow Connect PM |
| Vinay Kumar | Developer running the POC; hit the original blocking issue |
| Shankha Majumdar | POC team member |
| George Findling | Participant asking clarifying networking/cost questions |
| **Absent/mentioned:** | Rahul (organizer, last-minute emergency), MG, Mohamed, Brian, Karthik/Karthikeyan (infra/network team), Jeff, Riyaz (has workspace access) |

---

## Purpose

Vinay's team hit a blocking issue running a **Lakeflow Connect query-based connector POC** against on-prem SQL Server. Rahul convened this call with the database/platform experts to identify viable options for connectivity and compute so the POC can move forward.

---

## Current-State Architecture (as established in the meeting)

- **Lakeflow Connect managed ingestion (MI)** is in use today: an **ingestion gateway runs on classic compute** and connects to the on-prem SQL Server source; the downstream **ingestion pipeline runs on serverless** in the Databricks backend. Stephen initially believed the whole path was serverless — clarified by Venkata that **the on-prem-facing hop (the gateway) is classic compute**, which is why it works with the existing firewall posture. (The pipeline backend being serverless was confirmed separately.)
- Egress today: classic compute → **NAT gateway → static public IP → on-prem/DMZ firewall rule allowing that static IP**.
- The **DLX workspace is on a routed VNet** (directly connected to the DMZ network). The earlier **WAF workspace was on a non-routed VNet**, which compounded its connectivity problems.

## The Problem

The team is piloting the **query-based connector** (change-tracking style: cursor columns + table list — i.e., no dependence on SQL Server CDC). But:

1. **Query-based connector requires serverless compute** to reach the source (classic compute support exists only in **beta**).
2. **Serverless egress uses dynamic IPs.** The on-prem firewall only permits the known static IP from the classic-compute NAT gateway, so serverless connections are blocked. There is no static IP/range to give the firewall team.
3. This exact failure was already experienced in the **WAF workspace**: serverless → dynamic IP → blocked at DMZ firewall. The infra/network team (Karthik) **explicitly recommended against serverless** for on-prem connectivity ("serverless is managed by the cloud [vendor]"); the working resolution was classic compute + NAT gateway static IP.

### Why CDC is in scope at all (context from sponsor)

Mostly implicit in the transcript (Stephen at ~15:12: "CDC goes down from SQL Server… okay, change tracking… query-based"). Per environment context outside this transcript: **source-side CDC maintenance burden is high**, and ingestion breakage has at times forced source systems to stop CDC. The query-based/change-tracking connector is attractive precisely because it lightens the source footprint.

---

## Key Discussion Points

### 1. Serverless vs classic compute — policy and economics (Stephen)
- Serverless is enabled everywhere but used **strategically**: cheaper for **short-lived jobs** (no payment for cluster startup), more expensive and less controllable for long-running jobs, where a **tuned classic cluster beats serverless on cost**.
- Serverless: Databricks dynamically sizes compute; you can't pick VMs/region placement. Classic: prescriptive control over VM types/region.
- George's takeaway: the trade-off isn't just **cost**, it's **access/network control**.

### 2. Network/security concern with serverless (Stephen, Venkata)
- Opening the on-prem firewall to "all of Databricks' data center" range is unacceptable; the question is **how narrowly the firewall could be scoped** for serverless to work.
- Venkata's WAF lesson-learned (detailed): serverless dynamic IP → reaches DMZ VNet → blocked, because the allow-rule is keyed to the **NAT gateway's static IP**, which serverless traffic does not traverse. Even attaching a NAT gateway to serverless doesn't help — traffic still presents dynamic IPs.

### 3. Private Link as the serverless remedy (Kevin, Stephen)
- Kevin (Databricks): the supported pattern is **Private Link from serverless to the source database** — this is how Databricks would configure serverless → on-prem connectivity.
- Stephen is cautiously receptive ("would resolve it… am I thinking of the right technology?") but wary it amounts to a workaround with its own security review burden.
- **Venkata's core ask of this meeting:** does any existing environment already have serverless → on-prem connectivity established that could be copied? (Kevin raised checking for Walgreens examples/documentation, e.g., Private Link setup.) Stephen's answer: **no — serverless is deliberately avoided for integration connections**; none identified on this call (the people who might know more — MG, Rahul — were absent).

### 4. Classic compute for query-based connector — status (Kevin)
- Kevin spoke with the **Lakeflow Connect product manager**: classic compute support for query-based connectors is **moving to Public Preview "really soon"** — hinted to land at the **Databricks Summit (~2 weeks out)** — and the PM **okayed it for production workloads** now.
- Caveats: **no DAB (Databricks Asset Bundles) support** — pipeline must be created **via UI or JSON API**, specifying cursor columns, tables, and classic compute (`serverless: false`).

### 5. POC blockers encountered (Vinay)
- Initial code (serverless) worked. Adding a job-cluster definition to the DAB produced: *"you cannot provide the cluster setting when using serverless compute with [the] pipeline."* A Genie-suggested code modification failed with the same error.
- Root cause per Kevin: **you can't do this through a DAB yet** — the team was attempting an unsupported path. Classic compute had also been reported failing this morning (consistent with the DAB limitation, not necessarily the API path).

---

## Decisions & Direction

1. **Stop pursuing serverless → on-prem for integration connections** for now; Stephen now agrees with Karthikeyan/infra. Serverless stays reserved for "outlier processing," not source connectivity.
2. **Try the classic compute route for the query-based connector** via UI/JSON API (not DAB) — for a quick POC, one table first.
3. Private Link remains a candidate **option to be written up**, not actioned yet.

## Action Items

| # | Action | Owner | Notes |
|---|---|---|---|
| 1 | Write up pros/cons of up to **4 options**: (a) Private Link for serverless, (b) classic compute / public preview path, (c) **source-side changes** (source team has "a couple options"), (d) TBD | Kevin Nguyen (w/ Mohamed absent) | Bring back to the table |
| 2 | Working session later today: create query-based pipeline on **classic compute via UI/API**, single table, walkthrough | Kevin + Venkata + Riyaz (has access) | Vinay/Shankha to share existing DAB/code |
| 3 | Share network topology diagram (routed vs non-routed VNet, NAT, DMZ) | Venkata | From network team |
| 4 | Provide sample/reference code for the classic-compute query-based pipeline | Kevin | Raised post-meeting by Vinay/Venkata |

## Open Questions / Risks

- **Public preview timing risk:** the classic-compute GA path is being bet on a PM's verbal "okay" and a conference announcement. If it slips, the POC is stuck between unsupported-beta and blocked-serverless.
- **No DAB support** for the classic-compute query-based pipeline → breaks the team's CI/CD deployment standard (UI/API-created pipelines are config drift risks). Needs an interim IaC approach (scripted JSON API calls).
- **Private Link scope:** unresolved whether Private Link from serverless can be scoped acceptably for the security team, and whether any precedent exists in the org.
- **Source-side options were named but never enumerated** on this call (option c). Given the strategic problem — CDC burden on the 14 busy NEXiA SQL Servers (environment context, not discussed in this transcript) — this is arguably the most important branch and deserves its own session.
- Transcript gaps (~1–9 min, ~11–15 min) likely contained Kevin's initial recommendation detail and part of the architecture walkthrough; confirm nothing material was lost.

---

## Architect's Commentary

*(Observations from a data-architecture lens; not stated verbatim in the meeting.)*

1. **The query-based connector quietly changes the source contract.** Moving from CDC to change tracking/cursor-column queries shifts load from *log-capture maintenance* to *query-time scans on the source*. That relieves the CDC log/cleanup burden that has been breaking things, but on busy OLTP boxes the polling queries need careful design (indexed cursor columns, off-peak full loads, snapshot isolation to avoid CT cleanup races). It also loses intermediate change history and hard-delete capture unless CT is used properly — verify the connector's delete semantics per table.
2. **The compute debate is really an egress-identity debate.** Every option reduces to "what stable network identity does Databricks present to the on-prem firewall?" Classic+NAT answers it with a static IP; Private Link answers it by removing the public path entirely. Private Link is the architecturally cleaner end-state; classic+NAT is the pragmatic now-state.
3. **Nobody on the call connected this POC to the Fabric landing zone.** If Fabric (via mirroring/Lakeflow-equivalent paths) is the primary analytics plane, running *parallel* CDC/CT consumers from both Fabric and Databricks against the same busy sources will multiply source burden — the exact problem being solved. An "extract once, share downstream" decision is needed at the architecture level (see companion document).
4. **Beta/preview dependency management:** good instinct from the team to wait for public preview before production. Capture the PM commitment in writing through the account team.
