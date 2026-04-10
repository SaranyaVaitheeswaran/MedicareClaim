/**
 * DAT 226 - NoSQL Homework
 * Medicare Provider Billing Anomaly Detection
 * MongoDB Baseline Analytics Queries
 * 
 * These 5 queries provide descriptive analytics and baseline
 * understanding of the dataset before anomaly detection.
 * 
 * Collection: medicare_db.provider_services
 * Dataset: CMS Medicare Physician and Other Practitioners (40K records)
 */

// ─────────────────────────────────────────────────────────
// Query 1: Top 10 Provider Types by Total Beneficiaries
// Insight: Clinical Laboratory and Diagnostic Radiology serve
// the most beneficiaries. Ambulance Service Provider has
// disproportionately high service counts relative to beneficiaries
// suggesting repeat/ongoing services per patient.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$Rndrng_Prvdr_Type",
      total_beneficiaries: { $sum: "$Tot_Benes" },
      total_services: { $sum: "$Tot_Srvcs" },
      provider_count: { $sum: 1 }
    }
  },
  { $sort: { total_beneficiaries: -1 } },
  { $limit: 10 }
]);

// ─────────────────────────────────────────────────────────
// Query 2: Top 20 HCPCS Services by Total Service Count
// Insight: Drug injection codes (J-series) dominate high
// service counts with low beneficiary counts — indicating
// repeat treatments per patient. J0897 (denosumab injection)
// had 181,425 services for only 1,768 beneficiaries.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$HCPCS_Cd",
      hcpcs_desc: { $first: "$HCPCS_Desc" },
      total_services: { $sum: "$Tot_Srvcs" },
      total_beneficiaries: { $sum: "$Tot_Benes" }
    }
  },
  { $sort: { total_services: -1 } },
  { $limit: 20 }
]);

// ─────────────────────────────────────────────────────────
// Query 3: Average Charge-Payment Gap by Provider Type
// Insight: Ambulatory Surgical Centers have the highest
// absolute charge-payment gap ($5,510) but Anesthesiology
// has a higher Gap_Ratio (0.917) — meaning anesthesia
// providers keep less of what they charge proportionally.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$Rndrng_Prvdr_Type",
      avg_charge_payment_gap: { $avg: "$Charge_Payment_Gap" },
      avg_gap_ratio: { $avg: "$Gap_Ratio" },
      avg_submitted_charge: { $avg: "$Avg_Sbmtd_Chrg" },
      avg_payment: { $avg: "$Avg_Mdcr_Pymt_Amt" },
      provider_count: { $sum: 1 }
    }
  },
  { $sort: { avg_charge_payment_gap: -1 } },
  { $limit: 15 }
]);

// ─────────────────────────────────────────────────────────
// Query 4: Rural vs Urban Providers (RUCA-based)
// Insight: Rural providers show different payment patterns
// from urban counterparts. RUCA codes classify metropolitan
// vs micropolitan vs rural areas — enabling geographic
// healthcare access analysis.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$Rndrng_Prvdr_RUCA_Desc",
      avg_payment: { $avg: "$Avg_Mdcr_Pymt_Amt" },
      avg_submitted_charge: { $avg: "$Avg_Sbmtd_Chrg" },
      total_services: { $sum: "$Tot_Srvcs" },
      total_beneficiaries: { $sum: "$Tot_Benes" },
      provider_count: { $sum: 1 },
      avg_gap_ratio: { $avg: "$Gap_Ratio" }
    }
  },
  { $sort: { avg_payment: -1 } }
]);

// ─────────────────────────────────────────────────────────
// Query 5: Medicare Participating vs Non-Participating
// Discrepancy Pattern Comparison
// Insight: Non-participating providers (id: "N") show higher
// avg_submitted_charge ($644) vs participating (id: "Y") ($63),
// but participating providers have higher Gap_Ratio (0.735 vs 0.415)
// — suggesting participating providers submit more aggressively
// relative to what Medicare pays them.
// ─────────────────────────────────────────────────────────

db.provider_services.aggregate([
  {
    $group: {
      _id: "$Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
      avg_submitted_charge: { $avg: "$Avg_Sbmtd_Chrg" },
      avg_allowed_amount: { $avg: "$Avg_Mdcr_Alowd_Amt" },
      avg_payment: { $avg: "$Avg_Mdcr_Pymt_Amt" },
      avg_charge_payment_gap: { $avg: "$Charge_Payment_Gap" },
      avg_gap_ratio: { $avg: "$Gap_Ratio" },
      avg_submitted_allowed_ratio: { $avg: "$Submitted_Allowed_Ratio" },
      total_beneficiaries: { $sum: "$Tot_Benes" },
      total_services: { $sum: "$Tot_Srvcs" },
      provider_count: { $sum: 1 }
    }
  },
  { $sort: { avg_gap_ratio: -1 } }
]);
