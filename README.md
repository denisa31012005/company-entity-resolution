# Company Entity Resolution (Dedup) — Safer Version (Anti Over-Merge)

This project performs **company entity resolution / deduplication** at scale using **PySpark**, producing:
1) a stable `record_id` per row  
2) an `entity_id` that groups records believed to represent the same real-world company  
3) an evidence/audit table describing *why* records were linked

It is intentionally tuned to be **conservative** (reduce “over-merging”), using **blocking + deterministic matches + guarded fuzzy matching**, plus **cluster sanity checks**.

---

## Results (quality + quick validation)

> Fill the placeholders below with numbers from your run; this section is what reviewers look for first.

- **Input records:** `<N_records>`
- **Unique entities produced:** `<N_entities>`
- **Dedup reduction:** `<(1 - N_entities/N_records) * 100>%`
- **Largest cluster size:** `<max_cluster_size>`
- **Sanity flags:** `<N_suspicious_clusters>` clusters flagged by over-merge detectors

**Precision proxy (lightweight checks):**
- Manually inspected the **top `<K>` largest clusters** and a random sample of `<M>` clusters:
  - **Obvious false merges:** `<count>` / `<sample_size>`
  - **Obvious false splits (missed merges):** `<count>` / `<sample_size>`
- Reviewed the **evidence table** for flagged clusters to confirm which keys/edges caused links.

**Before/after examples (anonymize if needed):**
- Entity `<entity_id_1>`:  
  - Before: `["ACME LLC", "Acme, L.L.C.", "ACME"]`  
  - Shared evidence: `domain`, `phone`, `postcode` (name_score ~ `<score>`)
- Entity `<entity_id_2>`:  
  - Before: `["Foo GmbH", "FOO GMBH Berlin"]`  
  - Shared evidence: `email`, `city+country` (name_score ~ `<score>`)

---

## What defines a “company” here (and why these attributes)

A real-world company is an organization that can appear multiple times across systems with inconsistent formatting, missing fields, and different naming conventions. In practice, **no single field is always a perfect identifier**, so this system combines:

**Strong identifiers (high precision, can still be shared):**
- **Website domain**: often unique for an organization, but can be shared across corporate groups, franchises, resellers, or web-hosting patterns.
- **Email address / email domain**: direct email addresses are strong, but shared inboxes (info@), or vendor-managed domains can cause collisions.
- **Phone number**: strong in many datasets, but call-centers, HQ numbers, or BPO providers can be reused.

**Weak identifier (needs corroboration):**
- **Company name**: highly variable (suffixes, punctuation, transliterations) and often **non-unique** (“Acme”, “Global Trading”).  
  → Therefore, name similarity is used only with corroborating signals (geo/postcode/email domain + country).

**Contextual attributes (used to reduce false merges):**
- **Country, city, postcode, coarse lat/lon**: not unique alone, but helpful to confirm “same org” when names are similar.

This design matches the task goal: **leverage the most relevant attributes**, and justify decisions with explicit reasoning.

---

## What the code does (high level)

### Pipeline stages
1. **Load input (Parquet)**
2. **Data profiling (completeness report)**
3. **Normalization / standardization** of key attributes (domain, names, phones, emails, geo)
4. **Blocking key generation** (multiple strategies)
5. **Pair generation (candidates)** via blocking (excluding huge blocks)
6. **Matching**
   - Deterministic “hard” edges (domain/phone/email with caps and constraints)
   - Fuzzy “soft” edges (RapidFuzz name similarity + corroboration rules)
7. **Graph clustering** via **connected components**
   - Implemented with a **label-propagation / min-label** iterative algorithm  
   - **GraphFrames is optional but disabled by default** to keep deployment simpler and avoid extra runtime dependencies (and because the min-label approach is portable and works well for this scale).
8. **Sanity checks & over-merge detectors**
9. **Write outputs**: mapping, updated dataset, evidence table

---

## Key concepts used

### 1) Distributed processing with Spark
- Uses **SparkSession** and Spark SQL/DataFrame transformations.
- Relies on **lazy evaluation**, then triggers execution with actions like `count()`, `show()`, writes.
- Performance tools:
  - `.cache()` / `.persist(StorageLevel.MEMORY_AND_DISK)` for reuse
  - `.repartition(200)` to control parallelism
  - `checkpoint()` to cut lineage (avoid huge DAGs and recomputation)

---

### 2) Data quality profiling (completeness report)
- Computes per-column **% non-null**:
  - `count(when(col(c).isNotNull(), 1)) * 100 / n`
- Uses `stack()` to reshape wide completeness results into a “pretty” long format table.

---

### 3) Feature normalization (standardization)
Normalization is used to reduce noise before matching.

**Website/domain**
- Lowercase + trim
- Extracts domain from URL with regex (remove protocol, `www`, path/query)
- Chooses best source (`website_domain` preferred; fallback to derived)
- Validates with a domain regex; invalid domains -> `null`

**Company names**
- Lowercase, remove punctuation, collapse whitespace
- Removes common **legal suffixes** (Inc, LLC, GmbH, SRL, etc.)
- Produces normalized variants:
  - `company_name_norm`
  - `company_commercial_names_norm`
  - `company_legal_names_norm`

**Phones**
- Strips non-digits (keeps `+`)
- Splits multi-phone strings into arrays (supports separators like `| , ;`)
- Builds normalized phone arrays and a “best available” phone (`phone_any_norm`)

**Emails**
- Lowercase + regex validate
- Extracts email domain (`email_domain`)

**Geography / address**
- Normalizes country code, city, postcode, street fields
- Casts lat/lon to double and rounds to 3 decimals for coarse geo matching

---

### 4) Blocking (candidate reduction)
Instead of comparing every record to every other record (quadratic explosion), the code builds **blocking keys** to generate likely candidate pairs.

Blocking keys include:
- Domain-based: `dom:<domain>`
- Phone-based: `ph:<phone>` (one per phone + a representative phone)
- Email-based: `em:<email>`
- Email-domain + name head: `emd:<email_domain>|<first_token_of_name>`
- Geo-based:
  - `geo1:<country>|<city>`
  - `geo2:<country>|<postcode>`
  - `geo3:<lat_r3>|<lon_r3>`
- Name-based:
  - first 1–2 tokens “fingerprint” (`name_fp`)
  - `nm1:<country>|<name_fp>`
  - `nm2:<country>|<name_fp>|<postcode>`

It also:
- removes null keys (`filter`)
- deduplicates keys (`array_distinct`)
- inspects block sizes to understand risk (large blocks are dangerous)

**Note on performance:** the fuzzy name UDF is comparatively expensive, so **blocking is doing the heavy lifting** by limiting UDF evaluation to a small candidate set rather than the full Cartesian product.

---

## 5) Anti over-merge safeguards (core idea)

The code is designed to avoid “giant accidental clusters” by applying multiple guardrails:

**A) Key size caps for deterministic links**
- Do not link on a key if it appears too often:
  - `MAX_DOMAIN_KEY_SIZE`
  - `MAX_PHONE_KEY_SIZE`
  - `MAX_EMAIL_KEY_SIZE`
This prevents shared call-centers, shared domains, or common emails from merging huge populations.

**B) Country constraints for deterministic links**
- Domain edges: require **same domain AND same country**
- Phone edges: require **same phone AND same country**
- Email edges: require **same email AND same country**
This reduces cross-country false merges for multinationals or shared contact points.

**C) Block size cap for candidate generation**
- `MAX_BLOCK_SIZE` removes huge blocks before pair-building.
This prevents generating billions of candidate pairs and also reduces low-quality “everyone with same weak key” comparisons.

---

## 6) Matching strategy (edges in a graph)

### 6.1 Deterministic edges (exact matches)
Creates “hard” links (edges) between record pairs:
- `det_domain_cc` (domain + country)
- `det_phone_cc`  (phone + country)
- `det_email_cc`  (email + country)

Each edge includes:
- `match_type`
- `score` (100 for deterministic)
- human-readable `reason` for auditing

### 6.2 Fuzzy edges (name similarity + corroboration)
- Uses **RapidFuzz** `token_set_ratio` via a Spark **UDF** to compute `name_score`.
- A pair becomes a match only if:
  - `name_score >= 90`
  - AND at least one strong corroborator holds:
    - same postcode, OR
    - same country + same city, OR
    - same rounded geocoordinates, OR
    - same email domain **AND** same country (explicitly *not allowed alone*)

This “AND corroboration” rule is the main safety mechanism preventing name-only merges (e.g., “Acme” in many places).

---

## 7) Graph-based clustering (connected components)

After producing edges, the dedup task becomes:
> “Find clusters of records connected by match edges.”

Implementation details:
- Builds an undirected graph by unioning edges with reversed direction.
- Uses an iterative **label propagation / union-find-like** approach:
  - initialize label = own id
  - each iteration: propagate labels over edges
  - take the minimum label per vertex
  - repeat up to `MAX_ITERS`

**Why connected components is acceptable (and the downside):**
- **Pro:** It is simple, scalable, and maps naturally to “linked by evidence edges.”
- **Con:** Connected components can **over-merge through chaining** (A matches B, B matches C → A and C end up in the same entity even if they wouldn’t match directly).
- **Mitigation in this project:** edge creation is intentionally conservative (caps + country constraints + corroborated fuzzy rules) and clusters are post-validated (size + name diversity checks) to surface suspicious merges.

Result:
- `entity_id` is the final cluster label (string)

---

## 8) Cluster sanity checks (over-merge detection)

After clustering:
- Checks if any records missed an `entity_id` and assigns fallback `entity_id = record_id`.

Diagnostics:
- **Cluster size distribution** (largest clusters first)
- **Name diversity ratio** within cluster:
  - `distinct_name_fp / n`
- Flags suspicious clusters:
  - big clusters (n ≥ 20) with high name diversity (> 0.4)

Also prints sample rows from top clusters to manually inspect.

---

## 9) Auditability (evidence table)

Builds an evidence dataset linking:
- `entity_id`, `src`, `dst`
- `match_type`, `score`, `reason`

Keeps only edges that are internal to a final cluster (both endpoints share same entity).  
This supports explainability, debugging, and manual review of flagged clusters.

---

## Known failure modes & mitigations

No dedup system is perfect; these are the primary risks and how this project handles them:

- **Corporate groups / shared domains** (parent + subsidiaries on same domain)  
  - *Risk:* merging separate legal entities into one entity.  
  - *Mitigation:* key size caps + country constraints + corroboration for fuzzy edges; flagged by diversity checks.

- **Franchises / resellers** (shared brand name, similar websites)  
  - *Risk:* name-based merges across locations.  
  - *Mitigation:* name-only merges are disallowed; geo corroboration required.

- **Shared phone numbers** (call centers, HQ, outsourced support)  
  - *Risk:* large accidental clusters.  
  - *Mitigation:* key caps + country constraint; large blocks filtered.

- **Multilingual / transliterated names**  
  - *Risk:* missed merges (false splits).  
  - *Mitigation:* token-set similarity helps some; can be extended with additional name normalization/aliases.

- **Sparse records** (missing domain/phone/email/geo)  
  - *Risk:* low recall.  
  - *Mitigation:* multiple blocking strategies; still expected to miss some merges when data is too incomplete.

---

## Inputs / Outputs

### Input
- `veridion.parquet` (company records)

### Outputs (Parquet)
1. `entity_map_parquet`
   - columns: `record_id`, `entity_id`
2. `veridion_with_entity_id_parquet`
   - original raw columns + `record_id` + `entity_id`
   - uses `df_raw` to avoid leaking engineered feature columns
3. `entity_edges_evidence_parquet`
   - columns: `entity_id`, `src`, `dst`, `match_type`, `score`, `reason`

---

## Repro / how to run

> Adjust paths, Spark settings, and main entrypoint names to your repo.

### Local (example)
```bash
pip install -r requirements.txt
python -m entity_resolution.run \
  --input parquet:///path/to/veridion.parquet \
  --out_dir parquet:///path/to/output_dir
