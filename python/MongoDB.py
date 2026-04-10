import pandas as pd
import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint

# Load environment variables
load_dotenv()

# Convert csv to json. NOSQL databases are ideal for semi structured data like JSON

def convert_csv_to_json(csv_path, json_path, sample_size=1000):
    print("=" * 60)
    print("STEP 1: Converting CSV to JSON (Semi-Structured Data)")
    print("=" * 60)

    df = pd.read_csv(csv_path, low_memory=False)
    print(f"Original CSV: {len(df)} rows, {len(df.columns)} columns")

    # Take a sample for JSON demo
    sample_df = df.head(sample_size)

    # Convert to JSON records
    records = sample_df.to_dict(orient='records')

    # Handle NaN values
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    with open(json_path, 'w') as f:
        json.dump(records, f, indent=2, default=str)

    print(f"Converted {len(records)} records to JSON: {json_path}")
    print(f"Sample JSON record structure:")
    print(json.dumps(records[0], indent=2, default=str)[:500] + "...")
    print()
    return records

def load_json_to_mongodb(records, collection_name="provider_services_json"):

    print("=" * 60)
    print("STEP 2: Loading JSON into MongoDB via PyMongo")
    print("=" * 60)

    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["medicare_db"]
    collection = db[collection_name]

    # Drop existing collection for clean demo
    collection.drop()

    # Insert JSON records
    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} JSON documents into '{collection_name}'")
    print(f"MongoDB auto-generated _id for each document — no schema definition needed")
    print(f"Total documents in collection: {collection.count_documents({})}")
    print()

    return client, db, collection

def run_query1(collection):
    """
    Query 1: Peer Z-Score Outlier Detection
    Find providers whose Gap_Ratio exceeds 2 standard deviations
    above their specialty+state peer group average.
    """
    print("=" * 60)
    print("QUERY 1: Peer Z-Score Outlier Detection")
    print("Find providers > 2 std devs above peer group Gap_Ratio")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {
                    "provider_type": "$Rndrng_Prvdr_Type",
                    "state": "$Rndrng_Prvdr_State_Abrvtn"
                },
                "avg_gap_ratio": {"$avg": "$Gap_Ratio"},
                "std_gap_ratio": {"$stdDevPop": "$Gap_Ratio"}
            }
        },
        {
            "$lookup": {
                "from": collection.name,
                "let": {
                    "grp_type": "$_id.provider_type",
                    "grp_state": "$_id.state",
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
                                    {"$gt": ["$Gap_Ratio", {
                                        "$add": ["$$grp_avg", {"$multiply": [2, "$$grp_std"]}]
                                    }]}
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "npi": "$Rndrng_NPI",
                            "name": "$Rndrng_Prvdr_Last_Org_Name",
                            "provider_type": "$Rndrng_Prvdr_Type",
                            "state": "$Rndrng_Prvdr_State_Abrvtn",
                            "gap_ratio": "$Gap_Ratio"
                        }
                    },
                    {"$sort": {"gap_ratio": -1}},
                    {"$limit": 5}
                ],
                "as": "outliers"
            }
        },
        {"$unwind": "$outliers"},
        {"$replaceRoot": {"newRoot": "$outliers"}},
        {"$sort": {"gap_ratio": -1}},
        {"$limit": 10}
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} outlier providers:")
    for r in results:
        print(f"  {r.get('name', 'N/A')} | {r.get('provider_type', 'N/A')} | "
              f"{r.get('state', 'N/A')} | Gap_Ratio: {r.get('gap_ratio', 0):.4f}")
    print()
    return results
def run_query2(collection):
    """
    Query 2: Drug vs Non-Drug Procedure Comparison
    """
    print("=" * 60)
    print("QUERY 2: Drug vs Non-Drug Procedure Comparison")
    print("=" * 60)

    pipeline = [
        {"$match": {"HCPCS_Drug_Ind": {"$in": ["Y", "N"]}}},
        {
            "$group": {
                "_id": "$HCPCS_Drug_Ind",
                "avg_gap_ratio": {"$avg": "$Gap_Ratio"},
                "avg_submitted_charge": {"$avg": "$Avg_Sbmtd_Chrg"},
                "avg_payment": {"$avg": "$Avg_Mdcr_Pymt_Amt"},
                "record_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "drug_indicator": "$_id",
                "avg_gap_ratio": {"$round": ["$avg_gap_ratio", 4]},
                "avg_submitted_charge": {"$round": ["$avg_submitted_charge", 2]},
                "avg_payment": {"$round": ["$avg_payment", 2]},
                "record_count": 1
            }
        },
        {"$sort": {"avg_gap_ratio": -1}}
    ]

    results = list(collection.aggregate(pipeline))
    print("Drug vs Non-Drug comparison:")
    for r in results:
        indicator = "Drug" if r.get('drug_indicator') == 'Y' else "Non-Drug"
        print(f"  {indicator}: Gap_Ratio={r.get('avg_gap_ratio')}, "
              f"Avg Charge=${r.get('avg_submitted_charge')}, "
              f"Avg Payment=${r.get('avg_payment')}, "
              f"Count={r.get('record_count')}")
    print()
    return results
def run_query3(collection):
    """
    Query 3: High Volume + High Gap Combination
    """
    print("=" * 60)
    print("QUERY 3: High Volume + High Gap Providers")
    print("Providers in top 25% of both Tot_Srvcs and Gap_Ratio")
    print("=" * 60)

    pipeline = [
        {
            "$match": {
                "Tot_Srvcs": {"$gt": 107.81},
                "Gap_Ratio": {"$gt": 0.899}
            }
        },
        {
            "$project": {
                "npi": "$Rndrng_NPI",
                "name": "$Rndrng_Prvdr_Last_Org_Name",
                "provider_type": "$Rndrng_Prvdr_Type",
                "state": "$Rndrng_Prvdr_State_Abrvtn",
                "tot_srvcs": "$Tot_Srvcs",
                "gap_ratio": {"$round": ["$Gap_Ratio", 4]},
                "avg_submitted": {"$round": ["$Avg_Sbmtd_Chrg", 2]},
                "avg_payment": {"$round": ["$Avg_Mdcr_Pymt_Amt", 2]}
            }
        },
        {"$sort": {"gap_ratio": -1, "tot_srvcs": -1}},
        {"$limit": 10}
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Top {len(results)} high volume + high gap providers:")
    for r in results:
        print(f"  {r.get('name', 'N/A')} | {r.get('provider_type', 'N/A')} | "
              f"{r.get('state', 'N/A')} | Services: {r.get('tot_srvcs')} | "
              f"Gap: {r.get('gap_ratio')}")
    print()
    return results
  def run_query4(collection):
    """
    Query 4: Facility vs Office Setting Comparison
    """
    print("=" * 60)
    print("QUERY 4: Facility vs Office Setting Comparison")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {
                    "hcpcs_cd": "$HCPCS_Cd",
                    "place_of_service": "$Place_Of_Srvc"
                },
                "avg_gap_ratio": {"$avg": "$Gap_Ratio"},
                "avg_submitted": {"$avg": "$Avg_Sbmtd_Chrg"},
                "avg_payment": {"$avg": "$Avg_Mdcr_Pymt_Amt"},
                "record_count": {"$sum": 1}
            }
        },
        {"$match": {"record_count": {"$gt": 5}}},
        {
            "$project": {
                "hcpcs_cd": "$_id.hcpcs_cd",
                "place": "$_id.place_of_service",
                "avg_gap_ratio": {"$round": ["$avg_gap_ratio", 4]},
                "avg_submitted": {"$round": ["$avg_submitted", 2]},
                "avg_payment": {"$round": ["$avg_payment", 2]},
                "record_count": 1
            }
        },
        {"$sort": {"avg_gap_ratio": -1}},
        {"$limit": 10}
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Top procedure-setting combinations by Gap_Ratio:")
    for r in results:
        setting = "Facility" if r.get('place') == 'F' else "Office"
        print(f"  HCPCS {r.get('hcpcs_cd')} | {setting} | "
              f"Gap: {r.get('avg_gap_ratio')} | "
              f"Charge: ${r.get('avg_submitted')} | "
              f"Payment: ${r.get('avg_payment')}")
    print()
    return results
    
def run_query5(collection):
    """
    Query 5: Credential-Based Anomaly Pattern
    """
    print("=" * 60)
    print("QUERY 5: Credential-Based Anomaly Pattern")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": "$Rndrng_Prvdr_Crdntls",
                "avg_gap_ratio": {"$avg": "$Gap_Ratio"},
                "avg_submitted": {"$avg": "$Avg_Sbmtd_Chrg"},
                "record_count": {"$sum": 1}
            }
        },
        {"$match": {"record_count": {"$gte": 10}}},
        {
            "$project": {
                "credentials": "$_id",
                "avg_gap_ratio": {"$round": ["$avg_gap_ratio", 4]},
                "avg_submitted": {"$round": ["$avg_submitted", 2]},
                "record_count": 1
            }
        },
        {"$sort": {"avg_gap_ratio": -1}},
        {"$limit": 15}
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Top credentials by average Gap_Ratio (min 10 records):")
    for r in results:
        print(f"  {str(r.get('credentials', 'N/A')):20s} | "
          f"Gap: {r.get('avg_gap_ratio')} | "
          f"Avg Charge: ${r.get('avg_submitted') or 0:10.2f} | "
          f"Count: {r.get('record_count')}")
    print()
    return results
# ─────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────

if __name__ == "__main__":

    CSV_PATH = "medical_final_40k_stratified_anomaly.csv"
    JSON_PATH = "medicare_sample.json"

    print("\n" + "=" * 60)
    print("DAT 226 - Medicare Billing Anomaly Detection")
    print("MongoDB Analytics using PyMongo")
    print("=" * 60 + "\n")

    # Step 1: Convert CSV to JSON
    records = convert_csv_to_json(CSV_PATH, JSON_PATH, sample_size=1000)

    # Step 2: Load JSON into MongoDB
    client, db, collection = load_json_to_mongodb(records)

    # Step 3: Run all 5 queries
    run_query1(collection)
    run_query2(collection)
    run_query3(collection)
    run_query4(collection)
    run_query5(collection)

    print("=" * 60)
    print("All queries completed successfully!")
    print("Source: github.com/SaranyaVaitheeswaran/MedicareClaim")
    print("=" * 60)

    client.close()
