# Medicare Provider Billing Anomaly Detection
## Using MongoDB Atlas, Neo4j Aura, and Claude Code with MCP

**DAT 226 — NoSQL and Data Warehousing | SJSU | April 2026**

---

## Project Overview

This project detects anomalous billing patterns in Medicare provider data using two complementary NoSQL databases:
- **MongoDB Atlas** — document analytics and aggregation pipelines
- **Neo4j Aura** — graph relationship analysis and network discovery

We also compared three AI tools for database query generation: Claude Code with MCP, MongoDB Atlas AI, and ChatGPT.

---

## Dataset

**Source:** CMS Medicare Physician and Other Practitioners by Provider and Service  
**URL:** https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service

**Sampling strategy:**
- 30,000 records: stratified sample across provider types
- 10,000 records: high-gap records based on Gap_Ratio
- Total: 40,000 records, 10,450 unique providers, 56 states, 3,147 cities

**Key engineered features:**
- `Gap_Ratio` = (Avg_Sbmtd_Chrg - Avg_Mdcr_Pymt_Amt) / Avg_Sbmtd_Chrg
- `Charge_Payment_Gap` = Avg_Sbmtd_Chrg - Avg_Mdcr_Pymt_Amt
- `Submitted_Allowed_Ratio` = Avg_Sbmtd_Chrg / Avg_Mdcr_Alowd_Amt

---

## Repository Structure

```
MedicareClaim/
├── data/
│   └── medical_final_40k_stratified_anomaly.csv  (hosted on GitHub)
├── mongodb/
│   ├── query1_peer_zscore.js
│   ├── query2_drug_vs_nondrug.js
│   ├── query3_high_volume_gap.js
│   ├── query4_facility_vs_office.js
│   └── query5_credential_analysis.js
├── neo4j/
│   └── 01_setup.cypher  (all LOAD CSV + relationship commands)
├── python/
│   ├── pymongo_queries.py
│   ├── neo4j_queries.py
│   └── .env.example
└── README.md
```

---

## Setup Instructions

### Prerequisites
- MongoDB Atlas free account: cloud.mongodb.com
- Neo4j Aura free account: console.neo4j.io
- Python 3.8+
- Node.js 18+

### Environment Variables
Create a `.env` file in the `python/` folder:
```
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/medicare_db
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=your-username
NEO4J_PASSWORD=your-password
```

### Install Python dependencies
```bash
pip install pymongo python-dotenv neo4j pandas
```

---

## MongoDB Setup

### 1. Create Atlas cluster
- Go to cloud.mongodb.com
- Create free M0 cluster
- Name it MedicareAnalysis

### 2. Import data
```bash
brew tap mongodb/brew
brew install mongodb-database-tools

mongoimport --uri "YOUR_ATLAS_URI" \
  --collection provider_services \
  --type csv \
  --headerline \
  --file medical_final_40k_stratified_anomaly.csv
```

### 3. Run aggregation queries
Open Atlas UI → Browse Collections → provider_services → Aggregations

---

## MongoDB Queries

### Query 1 — Peer Z-Score Outlier Detection
Finds providers whose Gap_Ratio exceeds 2 standard deviations above their specialty+state peer group.

```javascript
[
  {
    "$group": {
      "_id": {
        "Rndrng_Prvdr_Type": "$Rndrng_Prvdr_Type",
        "Rndrng_Prvdr_State_Abrvtn": "$Rndrng_Prvdr_State_Abrvtn"
      },
      "avg_gap_ratio": {"$avg": "$Gap_Ratio"},
      "std_gap_ratio": {"$stdDevPop": "$Gap_Ratio"}
    }
  },
  {
    "$lookup": {
      "from": "provider_services",
      "let": {
        "grp_type": "$_id.Rndrng_Prvdr_Type",
        "grp_state": "$_id.Rndrng_Prvdr_State_Abrvtn",
        "grp_avg": "$avg_gap_ratio",
        "grp_std": "$std_gap_ratio"
      },
      "pipeline": [
        {
          "$match": {
            "$expr": {
              "$and": [
                {"$eq": ["$Rndrng_Prvdr_Type", "$$grp_type"]},
                {"$eq": ["$Rndrng_Prvdr_State_Abrvtn", "$$grp_state"]},
                {"$gt": ["$Gap_Ratio", {"$add": ["$$grp_avg", {"$multiply": [2, "$$grp_std"]}]}]}
              ]
            }
          }
        },
        {
          "$project": {
            "Rndrng_NPI": 1,
            "Rndrng_Prvdr_Last_Org_Name": 1,
            "Rndrng_Prvdr_Type": 1,
            "Rndrng_Prvdr_State_Abrvtn": 1,
            "Gap_Ratio": 1
          }
        }
      ],
      "as": "outliers"
    }
  },
  {"$unwind": "$outliers"},
  {"$replaceRoot": {"newRoot": "$outliers"}},
  {"$sort": {"Gap_Ratio": -1}},
  {"$limit": 10}
]
```
**Key finding:** Gerndt (WI, Interventional Radiology) had Z-score ~2.90 — most statistically anomalous provider.

---

### Query 2 — Drug vs Non-Drug Comparison
```javascript
[
  {"$match": {"HCPCS_Drug_Ind": {"$in": ["Y", "N"]}}},
  {
    "$group": {
      "_id": "$HCPCS_Drug_Ind",
      "avgGapRatio": {"$avg": "$Gap_Ratio"},
      "avgSubmittedCharge": {"$avg": "$Avg_Sbmtd_Chrg"},
      "avgPayment": {"$avg": "$Avg_Mdcr_Pymt_Amt"},
      "recordCount": {"$sum": 1}
    }
  },
  {"$sort": {"avgGapRatio": -1}}
]
```
**Key finding:** Non-drug procedures (0.74) have higher Gap_Ratio than drug procedures (0.52) — Medicare drug pricing controls are more effective.

---

### Query 3 — High Volume + High Gap Providers
```javascript
[
  {
    "$match": {
      "Tot_Srvcs": {"$gt": 107.81},
      "Gap_Ratio": {"$gt": 0.899}
    }
  },
  {
    "$project": {
      "Rndrng_NPI": 1,
      "Rndrng_Prvdr_Last_Org_Name": 1,
      "Rndrng_Prvdr_Type": 1,
      "Rndrng_Prvdr_State_Abrvtn": 1,
      "Tot_Srvcs": 1,
      "Gap_Ratio": 1
    }
  },
  {"$sort": {"Gap_Ratio": -1, "Tot_Srvcs": -1}},
  {"$limit": 10}
]
```
**Key finding:** Mehta (NJ, Physical Medicine) had 1,396 services with Gap_Ratio 0.9949. Hematology-Oncology appeared 6 times.

---

### Query 4 — Facility vs Office Setting
```javascript
[
  {
    "$group": {
      "_id": {"HCPCS_Cd": "$HCPCS_Cd", "Place_Of_Srvc": "$Place_Of_Srvc"},
      "avgGapRatio": {"$avg": "$Gap_Ratio"},
      "avgSubmittedCharge": {"$avg": "$Avg_Sbmtd_Chrg"},
      "avgPayment": {"$avg": "$Avg_Mdcr_Pymt_Amt"},
      "totalRecords": {"$sum": 1}
    }
  },
  {"$match": {"totalRecords": {"$gt": 5}}},
  {"$sort": {"avgGapRatio": -1}},
  {"$limit": 20}
]
```
**Key finding:** HCPCS J2469 in office settings — submitted $61 but Medicare paid only $0.83 (97% gap).

---

### Query 5 — Credential Analysis
```javascript
[
  {
    "$group": {
      "_id": "$Rndrng_Prvdr_Crdntls",
      "avgGapRatio": {"$avg": "$Gap_Ratio"},
      "avgSubmittedCharge": {"$avg": "$Avg_Sbmtd_Chrg"},
      "recordCount": {"$sum": 1}
    }
  },
  {"$match": {"recordCount": {"$gt": 10}}},
  {"$sort": {"avgGapRatio": -1}},
  {"$limit": 15}
]
```
**Key finding:** Anesthesia credentials (C.R.N.A. 0.9477, CAA 0.9228, AA 0.9111) dominate top results.

---

## Neo4j Setup

### 1. Create Aura instance
- Go to console.neo4j.io
- Create free AuraDB instance
- Save the connection details file immediately

### 2. Run setup script
Copy and run each statement from `neo4j/01_setup.cypher` in the Neo4j Query editor one at a time.

The CSV is loaded directly from GitHub:
```
https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv
```

### Graph model
- **Nodes:** Provider (10,450), Service (~1,000), ProviderType, City (3,147), State (56)
- **Relationships:** PERFORMS (39,196), HAS_TYPE (10,450), LOCATED_IN_CITY (10,450), LOCATED_IN_STATE (10,450), IN_STATE (3,147)

---

## Neo4j Cypher Queries

### Query 1 — Services with Most High-Gap Providers (1 hop)
```cypher
MATCH (p:Provider)-[r:PERFORMS]->(s:Service)
WHERE r.gap_ratio > 0.95
RETURN s.hcpcs_cd, s.description,
       count(p) as high_gap_providers,
       avg(r.gap_ratio) as avg_gap,
       avg(r.avg_submitted_charge) as avg_charge
ORDER BY high_gap_providers DESC
LIMIT 10
```
**Key finding:** HCPCS 00731 (Anesthesia for esophagus endoscopy) had 76 high-gap providers.

---

### Query 2 — Provider Types by Gap (2 hops)
```cypher
MATCH (p:Provider)-[:HAS_TYPE]->(pt:ProviderType)
MATCH (p)-[r:PERFORMS]->(s:Service)
WHERE r.gap_ratio > 0.9
RETURN pt.name as provider_type,
       count(DISTINCT p) as providers,
       count(DISTINCT s) as services,
       avg(r.gap_ratio) as avg_gap_ratio
ORDER BY avg_gap_ratio DESC
LIMIT 10
```
**Key finding:** Nuclear Medicine (0.961) and Hematology-Oncology (0.954) top results — consistent with MongoDB findings.

---

### Query 3 — Geographic Clusters (3 hops)
```cypher
MATCH (p:Provider)-[:LOCATED_IN_CITY]->(c:City)-[:IN_STATE]->(s:State)
WHERE p.gap_ratio > 0.95
RETURN s.abbr as state,
       c.name as city,
       count(p) as anomalous_providers,
       avg(p.gap_ratio) as avg_gap_ratio
ORDER BY anomalous_providers DESC
LIMIT 10
```
**Key finding:** Houston TX (9 providers) and New York NY (8 providers) — geographic hotspots.

---

### Query 4 — Specialty + Location Clusters (4 hops)
```cypher
MATCH (p1:Provider)-[:HAS_TYPE]->(pt:ProviderType)<-[:HAS_TYPE]-(p2:Provider)
MATCH (p1)-[:LOCATED_IN_CITY]->(c:City)-[:IN_STATE]->(s:State)
WHERE p1.gap_ratio > 0.95
AND p2.gap_ratio > 0.95
AND p1.npi <> p2.npi
RETURN pt.name as specialty,
       c.name as city,
       s.abbr as state,
       count(DISTINCT p1) as anomalous_providers,
       avg(p1.gap_ratio) as avg_gap
ORDER BY anomalous_providers DESC
LIMIT 10
```
**Key finding:** CRNA clusters in Greenville SC, Detroit MI, Cincinnati OH, Erie PA — systemic nationwide pattern.

---

### Query 5 — Provider Similarity Network (5 hops)
```cypher
MATCH (p1:Provider)-[:HAS_TYPE]->(pt:ProviderType)<-[:HAS_TYPE]-(p2:Provider)
MATCH (p1)-[:LOCATED_IN_STATE]->(s:State)<-[:LOCATED_IN_STATE]-(p2)
MATCH (p1)-[:PERFORMS]->(svc:Service)<-[:PERFORMS]-(p2)
WHERE p1.npi < p2.npi
AND p1.gap_ratio > 0.9
AND p2.gap_ratio > 0.9
RETURN p1.last_name as provider1,
       p2.last_name as provider2,
       pt.name as specialty,
       s.abbr as state,
       count(svc) as shared_services,
       avg(p1.gap_ratio + p2.gap_ratio)/2 as avg_gap
ORDER BY shared_services DESC
LIMIT 10
```
**Key finding:** Blunck + Backlas (Diagnostic Radiology, TX) share 27 common high-gap services — network discovery impossible in MongoDB without complex application logic.

---

## Claude Code + MCP Setup

### Install Claude Code
```bash
curl -fsSL https://claude.ai/install.sh | bash
```

### MCP Config (claude_mcp_config.json)
```json
{
  "mcpServers": {
    "mongodb": {
      "command": "npx",
      "args": ["-y", "mongodb-mcp-server"],
      "env": {
        "MONGODB_URI": "YOUR_ATLAS_URI"
      }
    },
    "neo4j": {
      "command": "npx",
      "args": ["-y", "@alanse/mcp-neo4j-server"],
      "env": {
        "NEO4J_URI": "neo4j+s://your-instance.databases.neo4j.io",
        "NEO4J_USERNAME": "your-username",
        "NEO4J_PASSWORD": "your-password",
        "NEO4J_DATABASE": "your-database"
      }
    }
  }
}
```

### Launch with both MCPs
```bash
claude --mcp-config claude_mcp_config.json
```

---

## AI Tool Comparison

| Tool | Used For | Result |
|------|----------|--------|
| Claude Code + MCP | 5 MongoDB queries + 5 Neo4j queries | Best — parallel execution, best practices, caught data quality issues |
| MongoDB Atlas AI | Query generation + charts | Mixed — failed on complex queries, worked with structured prompts |
| ChatGPT | Neo4j graph model design | Good — reasonable structure, needed manual refinement |
| Neo4j Generate with AI | Graph model from CSV | Poor — created single flat node |

---

## Key Findings

1. **Anesthesia specialties** consistently show highest billing discrepancies (Gap_Ratio 0.91-0.92)
2. **Non-drug procedures** have higher Gap_Ratio (0.74) than drug procedures (0.52)
3. **Diagnostic Radiology network in Texas** — Blunck connected to 4 providers sharing 27 high-gap services
4. **CRNA clusters in 4 cities** nationwide — systemic pattern not isolated cases
5. **Claude Code outperformed Atlas AI** — ran 5 parallel queries vs Atlas AI memory error on complex queries

---

## Team

[Ani Taraiya, Kavya Ayappan, Rohini Ramasheshu]

SJSU MS Applied Data Intelligence — DAT 226 Spring 2026
