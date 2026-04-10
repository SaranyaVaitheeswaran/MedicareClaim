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
        grp_
