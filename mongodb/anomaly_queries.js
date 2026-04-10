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
