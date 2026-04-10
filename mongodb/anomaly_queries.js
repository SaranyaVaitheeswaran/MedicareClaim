/**
 * DAT 226 - NoSQL Homework
 * Medicare Provider Billing Anomaly Detection
 * MongoDB Anomaly Detection Queries
 * 
 * These 5 queries use statistical techniques to identify
 * providers with unusual billing patterns compared to their peers.
 * 
 * Key concept: We use Gap_Ratio = (Submitted - Payment) / Submitted
 * as our primary anomaly signal. A high Gap_Ratio means a provider
 * submits significantly more than Medicare pays.
 * 
 * Collection: medicare_db.provider_services
 * Dataset: CMS Medicare Physician and Other Practitioners (40K records)
 */

// ─────────────────────────────────────────────────────────
// Query 1: Peer Z-Score Outlier Detection
// Finds providers whose Gap_Ratio exceeds 2 standard deviations
// above their specialty + state peer group average.
// Uses $stdDevPop (not $stdDevSamp) because we treat each peer
// group as the full population, not a sample.
// 
// Insight: Gerndt (WI, Interventional Radiology) had Z-score ~2.90
// in a very tight peer group (std = 0.015) — most statistically
// anomalous provider. Clinical Cardiac Electrophysiology appeared
// twice suggesting a specialty-level pattern.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: {
        Rndrng_Prvdr_Type: "$Rndrng_Prvdr_Type",
        Rndrng_Prvdr_State_Abrvtn: "$Rndrng_Prvdr_State_Abrvtn"
      },
      avg_gap_ratio: { $avg: "$Gap_Ratio" },
      std_gap_ratio: { $stdDevPop: "$Gap_Ratio" }
    }
  },
  {
    $lookup: {
      from: "provider_services",
      let: {
        grp_type: "$_id.Rndrng_Prvdr_Type",
        grp_state: "$_id.Rndrng_Prvdr_State_Abrvtn",
        grp_avg: "$avg_gap_ratio",
        grp_std: "$std_gap_ratio"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$Rndrng_Prvdr_Type", "$$grp_type"] },
                { $eq: ["$Rndrng_Prvdr_State_Abrvtn", "$$grp_state"] },
                { $gt: ["$Gap_Ratio", {
                  $add: ["$$grp_avg", { $multiply: [2, "$$grp_std"] }]
                }]}
              ]
            }
          }
        },
        {
          $project: {
            Rndrng_NPI: 1,
            Rndrng_Prvdr_Last_Org_Name: 1,
            Rndrng_Prvdr_Type: 1,
            Rndrng_Prvdr_State_Abrvtn: 1,
            Gap_Ratio: 1,
            avg_gap_ratio: "$$grp_avg",
            std_gap_ratio: "$$grp_std"
          }
        }
      ],
      as: "outliers"
    }
  },
  { $unwind: "$outliers" },
  { $replaceRoot: { newRoot: "$outliers" } },
  { $sort: { Gap_Ratio: -1 } },
  { $limit: 10 }
]);

// ─────────────────────────────────────────────────────────
// Query 2: Drug vs Non-Drug Procedure Comparison
// Compares billing discrepancy patterns between drug-flagged
// and non-drug procedures.
// 
// Best practice: $match placed first to filter nulls before
// grouping — single most important performance optimization.
// 
// Insight: Non-drug procedures (0.7448) have higher Gap_Ratio
// than drug procedures (0.5252) — Medicare drug pricing controls
// are more effective than procedure-based controls.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  { $match: { HCPCS_Drug_Ind: { $in: ["Y", "N"] } } },
  {
    $group: {
      _id: "$HCPCS_Drug_Ind",
      avgGapRatio: { $avg: "$Gap_Ratio" },
      avgSubmittedCharge: { $avg: "$Avg_Sbmtd_Chrg" },
      avgPayment: { $avg: "$Avg_Mdcr_Pymt_Amt" },
      recordCount: { $sum: 1 }
    }
  },
  {
    $project: {
      drug_indicator: "$_id",
      avgGapRatio: { $round: ["$avgGapRatio", 4] },
      avgSubmittedCharge: { $round: ["$avgSubmittedCharge", 2] },
      avgPayment: { $round: ["$avgPayment", 2] },
      recordCount: 1
    }
  },
  { $sort: { avgGapRatio: -1 } }
]);

// ─────────────────────────────────────────────────────────
// Query 3: High Volume + High Gap Combination
// Finds providers in top 25% of BOTH service volume AND
// billing discrepancy. High volume anomalies are more
// significant than low volume ones.
// 
// Thresholds calculated separately:
//   p75 Tot_Srvcs = 107.81
//   p75 Gap_Ratio = 0.899
// 
// Insight: Mehta (NJ, Physical Medicine) had 1,396 services
// with Gap_Ratio 0.9949. Hematology-Oncology appeared 6 times
// — systemic specialty pattern not isolated cases.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $match: {
      Tot_Srvcs: { $gt: 107.81 },
      Gap_Ratio: { $gt: 0.899 }
    }
  },
  {
    $project: {
      Rndrng_NPI: 1,
      Rndrng_Prvdr_Last_Org_Name: 1,
      Rndrng_Prvdr_Type: 1,
      Rndrng_Prvdr_State_Abrvtn: 1,
      Tot_Srvcs: 1,
      Gap_Ratio: 1,
      Avg_Sbmtd_Chrg: 1,
      Avg_Mdcr_Pymt_Amt: 1
    }
  },
  { $sort: { Gap_Ratio: -1, Tot_Srvcs: -1 } },
  { $limit: 10 }
]);

// ─────────────────────────────────────────────────────────
// Query 4: Facility vs Office Setting Comparison
// Groups by HCPCS code and place of service to compare
// Gap_Ratio between facility (F) and office (O) settings
// for the same procedures.
// 
// Best practice: $group before $match because count filter
// depends on computed values — cannot be pushed upstream.
// 
// Insight: HCPCS J2469 in office settings — submitted $61
// but Medicare paid only $0.83 (97% gap). Facility-based
// billing dominated the top results.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: {
        HCPCS_Cd: "$HCPCS_Cd",
        Place_Of_Srvc: "$Place_Of_Srvc"
      },
      avgGapRatio: { $avg: "$Gap_Ratio" },
      avgSubmittedCharge: { $avg: "$Avg_Sbmtd_Chrg" },
      avgPayment: { $avg: "$Avg_Mdcr_Pymt_Amt" },
      totalRecords: { $sum: 1 }
    }
  },
  { $match: { totalRecords: { $gt: 5 } } },
  {
    $project: {
      hcpcs_cd: "$_id.HCPCS_Cd",
      place_of_service: "$_id.Place_Of_Srvc",
      avgGapRatio: { $round: ["$avgGapRatio", 4] },
      avgSubmittedCharge: { $round: ["$avgSubmittedCharge", 2] },
      avgPayment: { $round: ["$avgPayment", 2] },
      totalRecords: 1
    }
  },
  { $sort: { avgGapRatio: -1 } },
  { $limit: 20 }
]);

// ─────────────────────────────────────────────────────────
// Query 5: Credential-Based Anomaly Pattern
// Groups by provider credentials, filters to credentials
// with 10+ records for statistical validity.
// 
// Data quality note: CRNA and C.R.N.A. are the same credential
// with inconsistent formatting — identified by Claude Code.
// A $addFields + $trim/$replaceAll normalization stage before
// $group would consolidate them for more accurate results.
// 
// Insight: Anesthesia credentials dominate top results —
// C.R.N.A. (0.9477), CAA (0.9228), AA (0.9111). Systemic
// billing complexity in anesthesia rather than individual fraud.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$Rndrng_Prvdr_Crdntls",
      avgGapRatio: { $avg: "$Gap_Ratio" },
      avgSubmittedCharge: { $avg: "$Avg_Sbmtd_Chrg" },
      recordCount: { $sum: 1 }
    }
  },
  { $match: { recordCount: { $gte: 10 } } },
  {
    $project: {
      credentials: "$_id",
      avgGapRatio: { $round: ["$avgGapRatio", 4] },
      avgSubmittedCharge: { $round: ["$avgSubmittedCharge", 2] },
      recordCount: 1
    }
  },
  { $sort: { avgGapRatio: -1 } },
  { $limit: 15 }
]);
