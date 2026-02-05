# Company Entity Resolution (Deduplication)


This project identifies **unique companies** and groups **duplicate company records** that came from multiple systems and may differ slightly (name variants, missing fields, formatting differences).  
The final output is:

1. **Updated dataset**: original records + `entity_id` (the deduplicated company identifier)
2. **Entity map**: `record_id -> entity_id`
3. **Evidence table**: explains why two records were linked

---

## 1) Problem & Goal

### Task
> Identify unique companies and group duplicate records accordingly.

Company records can vary:
- **Name variations:** “ACME Inc.” vs “Acme Incorporated”
- **Missing values:** one source has a domain, another has only phone
- **Different formatting:** phone/email/address stored inconsistently
- **Many-to-one:** the same company appears in multiple systems

### What is a “company” in this solution?
A company is treated as the same real-world entity if multiple strong identifiers match, like:
- **Website domain** (very strong in most cases)
- **Phone number** (strong but can be reused; needs safeguards)
- **Email address** (strong, but shared mailboxes exist)
- **Name similarity + geo evidence** (for cases with missing direct identifiers)

---

## 2) Approach Overview

This solution uses a common entity-resolution pattern:

1. **Read data** and create a stable `record_id`
2. **Data profiling** (completeness report) to understand what to trust
3. **Normalize** key fields (domain, name, phone, email, geo)
4. **Blocking**: create “candidate groups” so we don’t compare every row with every other row
5. **Matching**:
   - Deterministic links (domain/phone/email)
   - Fuzzy links (name similarity + location corroboration)
   - Includes **anti over-merge safeguards** (caps on key sizes, block size limits, stricter rules)
6. **Clustering**: convert links into final entities via **connected components**
7. **Outputs**:
   - Updated dataset with `entity_id`
   - Mapping table
   - Evidence/audit table

---

## 3) Why I chose Spark + this design

### Why Spark
Even though this dataset is not “billions of records”, Veridion mentioned scalability. Spark is a realistic choice for:
- distributed processing
- large joins
- array + string feature engineering
- building candidate pairs with blocking

### Why “graph-based” entity resolution
Matching creates **edges** between records that seem to be the same company.  
Then the entity clustering is done by finding **connected components**:

- If A matches B, and B matches C, then A/B/C belong to the same company group (entity).
- This is a standard and production-friendly approach.

---

## 4) Decisions & Reasoning (Important design choices)

### 4.1 I do NOT use all columns
Many fields are noisy or missing. I prioritized attributes that uniquely identify companies:
- **domain_final**
- **phone_numbers_norm_arr**
- **primary_email_norm**
- **best_name_norm**
- **country/city/postcode**
- **rounded geo coordinates**

These give good balance between:
- **precision** (avoid merging different companies)
- **recall** (still catch duplicates with missing fields)

### 4.2 “Anti over-merge” safeguards (very important)
Over-merge = accidentally merging different companies into one entity.  
I added safeguards to reduce that risk:

- **Key size caps**
  - Don’t link everything that shares a super common key
  - Example: generic domains or shared emails could create giant incorrect clusters

- **Block size caps**
  - Drop extremely common blocking keys (like “geo1:US|new york”)
  - Prevents large candidate explosion and accidental merges

- **Domain+country constraint**
  - Domain alone can be misleading in rare cases (franchise, multi-country orgs, etc.)
  - This makes deterministic domain links safer.

- **Fuzzy rules require corroboration**
  - Name similarity alone is not enough.
  - We require postcode OR (country+city) OR rounded coordinates OR (email domain + same country).


