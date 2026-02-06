# Company Entity Resolution Project

## 1.What this project is about

The dataset contains **company records imported from multiple systems**, so the same real-world company can show up many times with small differences (typos, missing fields, different formatting,different naming conventions).

My goal was to **identify unique companies** and **group duplicate records** together.  
To do that, I produced:

- a **stable `record_id`** (one per row, so every record is uniquely trackable)
- an **`entity_id`** (one per real company,used to group duplicates)
- an **evidence table** explaining **why** two records were linked 

A very important design choice:I intentionally built the pipeline to be safer against accidental over-merging.
In entity resolution,merging two different companies is usually worse than failing to merge duplicates, because over-merges create incorrect entities that are hard to fix later.

---

## 2. Understanding the task: What counts as the same company?

A real-world company is an organization that might appear across systems with:

- different formats (ex: “LLC” vs “L.L.C.”)
- different languages (ex:GmbH, SRL,SA)
- incomplete records (missing phone or missing domain)
- shared information 

So there is no single perfect key that always identifies a company.  
This is why entity resolution needs a mix of signals:

### Strong identifiers (have high precision, but still not always perfect)
- **Website domain**: often unique, but can be shared across groups, franchises
- **Email address**: strong, but can be shared (`info@`, `contact@`).
- **Phone number**: strong, but can be reused by call centers

### Weak identifier
- **Company name**: can be messy and not unique so I thought that name alone is risky.

### Context attributes (I used for confirmation)
- **Country, city, postcode, rounded geo-coordinates**: not unique alone, but very useful to confirm identity when name matches.

---

## 3. Why I used Spark (PySpark)

Even though the provided dataset might not be “billions of records”, entity resolution is naturally expensive because the naive approach is:

> compare every record with every other record (O(n²))

That becomes impossible quickly.

I used **PySpark** because it is designed for **large-scale distributed processing**, and it lets the same logic scale up much more easily:

- DataFrames + Spark SQL operations are optimized for big data
- Spark’s **lazy evaluation** helps build an efficient execution plan
- Caching/persisting avoids recomputing heavy transformations
- Repartitioning helps control parallelism
- Checkpointing helps avoid extremely long Spark DAG lineages

---

## 4. The main idea of my approach

I treated entity resolution like building a **graph**:

- each record = a **node**
- if two records match (based on strong rules or fuzzy rules)=an **edge**
- each final company entity= a **connected component** in that graph

This is a common and practical way to solve deduplication, because:
- real duplicates may not match directly,  but can connect through shared evidence
- it gives  a clean final grouping (`entity_id`) after edges are created

But connected components has one big risk: **chaining over-merges**  
(A matches B, B matches C → then A and C become the same entity).

That’s why I invested a lot of effort in safe edges and anti over-merge safeguards.

---

## 5. Pipeline overview 

### Step 1: Load the data and create a stable identifier
- Input: `veridion.parquet`
- I create `record_id` using `monotonically_increasing_id()`

Why I used this approach:
- I need a stable row identifier for joins,edges,and tracking
- it avoids using expensive window functions for unique IDs

---

### Step 2: Completeness report
I computed the percentage of non-null values per column.

Why I did this:
- In entity resolution, you need to know **what fields are actually usable**
- if email is mostly missing, it won’t help much
- if domain is frequently present, it becomes a strong signal

This step also helps explain *why* I prioritized some attributes.

---

### Step 3 :Normalization
Normalization is a core entity resolution concept:  
**make the same thing look the same before matching.**

I normalized these fields:

#### 3.1 Domain /website
- lowercasing and trimming
- extracting domain from URL (remove protocol, `www`, paths)
- validating domain format(invalid → null)
- choosing best domain source (`website_domain` preferred, fallback to extracted)

Why:
- one system may store `https://www.acme.com/path`, another stores `acme.com`
- matching only works if these become the same normalized value

#### 3.2 Company names
- lowercase
- remove punctuation
- collapse whitespace
- remove legal suffixes(Inc, LLC, GmbH, SRL, SA, etc.)

Why:
- “Gisinger GmbH” and “Gisinger” should be treated as the same name core
- suffixes are usually not meaningful for identity matching across systems

#### 3.3 Phones
- remove non-digit characters(keeping `+`)
- split multi-phone columns into arrays
- keep a representative phone (`phone_any_norm`) for faster matching

Why:
- phone formats differ a lot:`+1 (234) 567-890` vs `1234567890`

#### 3.4 Emails
- lowercase
- regex validate format
- extract email domain

Why:
- email domain is useful as a *supporting* signal

#### 3.5 Geography /address
- normalize country code,city, postcode
- cast lat/lon to numeric and round to 3 decimals (`lat_r3`, `lon_r3`)

Why:
- geographic confirmation is extremely useful to prevent false merges
- rounding geo makes it usable even when coordinates differ slightly

---

## 6. Blocking (candidate reduction)

Blocking is one of the most important techniques in entity resolution.

### Why blocking is needed in my project
Without blocking, comparing all pairs is quadratic and impossible for large datasets.

So instead, I created **blocking keys**:  
records only get compared if they share a block key.

### Blocking keys I used(and why)
I used multiple blocking strategies to increase recall while keeping efficiency:

- `dom:<domain>` — strong,high precision
- `ph:<phone>` — strong,but can be shared
- `em:<email>` — strong,but also capped later
- `emd:<email_domain>|<name_head>` —helps catch cases where exact email differs but same org domain exists
- `geo1:<country>|<city>` —location-based grouping
- `geo2:<country>|<postcode>` —tighter location grouping
- `geo3:<lat_r3>|<lon_r3>` — geo coordinate grouping
- `nm1:<country>|<name_fp>` —name fingerprint + country
- `nm2:<country>|<name_fp>|<postcode>` — even safer name grouping

Why multiple blocks:
- using multiple blocks avoids losing duplicates just because one field is missing

### Huge block filtering
I calculated block sizes and removed overly large blocks (`MAX_BLOCK_SIZE`).

Why:
- huge blocks create too many pairs
- huge blocks are usually low-quality (e.g:a very common city name or generic company name)
- filtering huge blocks improves performance and also reduces accidental merges

---

## 7. How I decide that  two records should link

I used a hybrid approach:

### 7.1 Deterministic matching (exact edges)
I created edges for exact matches of:
- domain +same country (`det_domain_cc`)
- phone +same country (`det_phone_cc`)
- email+ same country (`det_email_cc`)

Why same country:
- without country constraint, a shared domain or shared contact could merge across countries incorrectly

### Key-size caps (used for anti over-merge )
Even “strong identifiers” can be shared.  
So I added caps:

- `MAX_DOMAIN_KEY_SIZE`
- `MAX_PHONE_KEY_SIZE`
- `MAX_EMAIL_KEY_SIZE`

Why I added caps:
> if a domain/phone/email appears too many times,it is treated as unreliable and is not used for linking.

Why:
- common support numbers or shared domains can cause massive over-merges

---

### 7.2 Fuzzy matching
Fuzzy matching is mainly for names,because names vary.

I used **RapidFuzz** `token_set_ratio` to compute `name_score`.

Why token_set_ratio:
- it handles token order differences well
- it’s robust against small differences while being simple

#### The safe fuzzy rule I used
A fuzzy match only becomes an edge if:
- `name_score >= 90`
- and also at least one strong corroboration is true:
  - same postcode, or
  - same country + same city, or
  - same rounded geo coordinates, or
  - same email domain **and** same country 

Why I thought that this is important:
- name-only matching is dangerous
- requiring a second signal prevents merges based purely on similarity
- email domain is treated as weaker evidence and must be paired with geography/country

So fuzzy matching improves recall, but the rule design keeps precision high.

---

## 8. Clustering into final entities(connected components)

After creating match edges, the “duplicate grouping” becomes:
> find connected components in the graph

Implementation detail:
- I used a label-propagation iterative algorithm (doesn’t require GraphFrames)

Why I did it this way:
- GraphFrames adds extra dependency complexity
- the min-label approach is simple and works at scale in Spark
- it’s also easier to control and debug

Final result:
- every record gets an `entity_id` representing its connected component

---

## 9. Over-merge detection

Because connected components can chain merges, I added checks after clustering:

### Cluster size distribution
- largest clusters are inspected first
- very large clusters can indicate shared keys caused too much linking

### Name diversity ratio inside clusters
I computed:

- number of records in cluster (`n`)
- number of distinct name fingerprints (`distinct_name_fp`)
- `name_div_ratio = distinct_name_fp / n`

Then I flagged suspicious clusters:
- big clusters (`n >= 20`)
- high name diversity (`> 0.4`)

Why:
- a real company cluster usually has *similar* names
- a cluster with many different name fingerprints is likely an over-merge

This isn’t a perfect metric, but it’s a practical warning system.

---

## 10. Evidence table

I produced an evidence dataset that stores, for each edge:

- `entity_id`
- `src`, `dst`
- `match_type`
- `score`
- `reason`

Why I did this:
- in real entity resolution systems,explainability is critical

---

## 11. Outputs (deliverables)

### Required output: updated dataset
- `veridion_with_entity_id_parquet`
  - original dataset + `record_id` + `entity_id`

Why I used `df_raw` for the final dataset:
- I didn’t want to leak engineered columns (normalized features, block keys)
- deliverable should contain clean original fields +entity assignment

### Additional outputs
- `entity_map_parquet`
  - just `record_id`, `entity_id` (useful for joins)
- `entity_edges_evidence_parquet`
  - evidence table for explainability and validation

---

## 12. What I would improve next

Entity resolution always involves tradeoffs. 
If I extended this:

- better handling of corporate groups using additional org hierarchy signals
- smarter phone logic (e.g., recognize call-center patterns)
- cluster repair strategies (split suspicious clusters based on weak edges)

---

## 13. Summary of key concepts used

- **Spark / istributed processing** → needed for scalable ER and big pair operations
- **Normalization** →reduces formatting noise and increases match reliability
- **Blocking** →makes ER possible by avoiding O(n²) comparisons
- **Deterministic matching**→ high-precision edges for strong identifiers
- **Key-size caps**→ prevents shared identifiers from creating false clusters
- **Country constraints** →reduces cross-country false merges
- **Fuzzy name similarity** → handles name variation,but guarded for safety
- **Corroboration rules** →stops name-only merges, improves precision
- **Graph clustering** → natural way to group linked duplicates
- **Sanity checks** → detects likely over-merges
- **Evidence table** → explainability

---

## 14. How to run

1. Put `veridion.parquet` in the working directory
2. Run the PySpark script
3. Collect outputs:
   - `entity_map_parquet`
   - `veridion_with_entity_id_parquet`
   - `entity_edges_evidence_parquet`

---

## 15. Results section

- **Input records:** 33,446
- **Unique entities produced:** 7,941
- **Dedup reduction:** 1-(7941/33,446)-> ~76.3%
- **Largest cluster size:** 107
- **Suspicious clusters flagged:** 23

Manual checks performed:
- inspected top **5** largest clusters
- inspected a sample of **10** clusters
- checked the **evidence table** for flagged clusters

The results show a significant reduction in duplicate records, with roughly three quarters of the dataset being grouped into entities.

