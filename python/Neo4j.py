import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

class MedicareGraphAnalytics:

    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")

        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        print("Connected to Neo4j Aura successfully")
        print()

    def close(self):
        self.driver.close()

    def run_query(self, query, description, params=None):
        print("=" * 60)
        print(f"{description}")
        print("=" * 60)
        with self.driver.session() as session:
            result = session.run(query, params or {})
            records = list(result)
            print(f"Returned {len(records)} records:")
            return records
            
    def query1_high_gap_services(self):
        query = """
        MATCH (p:Provider)-[r:PERFORMS]->(s:Service)
        WHERE r.gap_ratio > 0.95
        RETURN s.hcpcs_cd AS hcpcs_cd,
               s.description AS description,
               count(p) AS high_gap_providers,
               round(avg(r.gap_ratio) * 10000) / 10000 AS avg_gap,
               round(avg(r.avg_submitted_charge) * 100) / 100 AS avg_charge
        ORDER BY high_gap_providers DESC
        LIMIT 10
        """
        records = self.run_query(
            query,
            "QUERY 1: Services with Most High-Gap Providers (1 hop)\n"
            "Provider -> PERFORMS -> Service"
        )
        for r in records:
            desc = r['description'][:40] if r['description'] else 'N/A'
            print(f"  {r['hcpcs_cd']} | {desc:40s} | "
                  f"Providers: {r['high_gap_providers']:3d} | "
                  f"Avg Gap: {r['avg_gap']:.4f} | "
                  f"Avg Charge: ${r['avg_charge']:10.2f}")
        print()
        return records

    # ─────────────────────────────────────────
    # Query 2: Provider Types + Services (2 hops)
    # ─────────────────────────────────────────

    def query2_provider_type_services(self):
        query = """
        MATCH (p:Provider)-[:HAS_TYPE]->(pt:ProviderType)
        MATCH (p)-[r:PERFORMS]->(s:Service)
        WHERE r.gap_ratio > 0.9
        RETURN pt.name AS provider_type,
               count(DISTINCT p) AS providers,
               count(DISTINCT s) AS services,
               round(avg(r.gap_ratio) * 10000) / 10000 AS avg_gap_ratio
        ORDER BY avg_gap_ratio DESC
        LIMIT 10
        """
        records = self.run_query(
            query,
            "QUERY 2: Provider Types with Highest Gap (2 hops)\n"
            "Provider -> HAS_TYPE -> ProviderType AND Provider -> PERFORMS -> Service"
        )
        for r in records:
            print(f"  {r['provider_type']:35s} | "
                  f"Providers: {r['providers']:3d} | "
                  f"Services: {r['services']:3d} | "
                  f"Avg Gap: {r['avg_gap_ratio']:.4f}")
        print()
        return records

    # ─────────────────────────────────────────
    # Query 3: Geographic Clusters (3 hops)
    # ─────────────────────────────────────────

    def query3_geographic_clusters(self):
        query = """
        MATCH (p:Provider)-[:LOCATED_IN_CITY]->(c:City)-[:IN_STATE]->(s:State)
        WHERE p.gap_ratio > 0.95
        RETURN s.abbr AS state,
               c.name AS city,
               count(p) AS anomalous_providers,
               round(avg(p.gap_ratio) * 10000) / 10000 AS avg_gap_ratio
        ORDER BY anomalous_providers DESC
        LIMIT 10
        """
        records = self.run_query(
            query,
            "QUERY 3: Anomalous Provider Geographic Clusters (3 hops)\n"
            "Provider -> LOCATED_IN_CITY -> City -> IN_STATE -> State"
        )
        for r in records:
            print(f"  {r['city']:20s}, {r['state']} | "
                  f"Anomalous Providers: {r['anomalous_providers']:3d} | "
                  f"Avg Gap: {r['avg_gap_ratio']:.4f}")
        print()
        return records

    # ─────────────────────────────────────────
    # Query 4: Specialty + Location Clusters (4 hops)
    # ─────────────────────────────────────────

    def query4_specialty_location_clusters(self):
        query = """
        MATCH (p1:Provider)-[:HAS_TYPE]->(pt:ProviderType)<-[:HAS_TYPE]-(p2:Provider)
        MATCH (p1)-[:LOCATED_IN_CITY]->(c:City)-[:IN_STATE]->(s:State)
        WHERE p1.gap_ratio > 0.95
          AND p2.gap_ratio > 0.95
          AND p1.npi <> p2.npi
        RETURN pt.name AS specialty,
               c.name AS city,
               s.abbr AS state,
               count(DISTINCT p1) AS anomalous_providers,
               round(avg(p1.gap_ratio) * 10000) / 10000 AS avg_gap
        ORDER BY anomalous_providers DESC
        LIMIT 10
        """
        records = self.run_query(
            query,
            "QUERY 4: Same Specialty + Same Location Clusters (4 hops)\n"
            "Provider -> HAS_TYPE -> ProviderType <- HAS_TYPE <- Provider\n"
            "+ Provider -> LOCATED_IN_CITY -> City -> IN_STATE -> State"
        )
        for r in records:
            print(f"  {r['specialty']:40s} | "
                  f"{r['city']:15s}, {r['state']} | "
                  f"Providers: {r['anomalous_providers']:2d} | "
                  f"Avg Gap: {r['avg_gap']:.4f}")
        print()
        return records

    # ─────────────────────────────────────────
    # Query 5: Provider Similarity Network (5 hops)
    # ─────────────────────────────────────────

    def query5_provider_similarity_network(self):
        query = """
        MATCH (p1:Provider)-[:HAS_TYPE]->(pt:ProviderType)<-[:HAS_TYPE]-(p2:Provider)
        MATCH (p1)-[:LOCATED_IN_STATE]->(s:State)<-[:LOCATED_IN_STATE]-(p2)
        MATCH (p1)-[:PERFORMS]->(svc:Service)<-[:PERFORMS]-(p2)
        WHERE p1.npi < p2.npi
          AND p1.gap_ratio > 0.9
          AND p2.gap_ratio > 0.9
        RETURN p1.last_name AS provider1,
               p2.last_name AS provider2,
               pt.name AS specialty,
               s.abbr AS state,
               count(svc) AS shared_services,
               round(((p1.gap_ratio + p2.gap_ratio) / 2) * 10000) / 10000 AS avg_gap
        ORDER BY shared_services DESC
        LIMIT 10
        """
        records = self.run_query(
            query,
            "QUERY 5: Provider Similarity Network (5 hops)\n"
            "SIMILAR_PROVIDER derived relationship — same specialty,\n"
            "same state, shared high-gap services"
        )
        for r in records:
            print(f"  {r['provider1']:15s} <-> {r['provider2']:15s} | "
                  f"{r['specialty']:30s} | "
                  f"{r['state']} | "
                  f"Shared Services: {r['shared_services']:3d} | "
                  f"Avg Gap: {r['avg_gap']:.4f}")
        print()
        return records
