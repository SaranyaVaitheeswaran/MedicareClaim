-- Step 1: Constraints
CREATE CONSTRAINT provider_npi IF NOT EXISTS 
FOR (p:Provider) REQUIRE p.npi IS UNIQUE;

CREATE CONSTRAINT service_code IF NOT EXISTS 
FOR (s:Service) REQUIRE s.hcpcs_cd IS UNIQUE;

CREATE CONSTRAINT state_abbr IF NOT EXISTS 
FOR (st:State) REQUIRE st.abbr IS UNIQUE;

-- Step 2: State nodes
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_Prvdr_State_Abrvtn IS NOT NULL
MERGE (s:State {abbr: row.Rndrng_Prvdr_State_Abrvtn});

-- Step 3: ProviderType nodes
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_Prvdr_Type IS NOT NULL
MERGE (pt:ProviderType {name: row.Rndrng_Prvdr_Type});

-- Step 4: Service nodes
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.HCPCS_Cd IS NOT NULL
MERGE (s:Service {hcpcs_cd: row.HCPCS_Cd})
SET s.description = row.HCPCS_Desc,
    s.drug_indicator = row.HCPCS_Drug_Ind;

-- Step 5: City nodes
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_Prvdr_City IS NOT NULL
MERGE (c:City {name: row.Rndrng_Prvdr_City, state: row.Rndrng_Prvdr_State_Abrvtn});

-- Step 6: Provider nodes
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_NPI IS NOT NULL
MERGE (p:Provider {npi: row.Rndrng_NPI})
SET p.last_name = row.Rndrng_Prvdr_Last_Org_Name,
    p.first_name = row.Rndrng_Prvdr_First_Name,
    p.credentials = row.Rndrng_Prvdr_Crdntls,
    p.entity_code = row.Rndrng_Prvdr_Ent_Cd,
    p.city = row.Rndrng_Prvdr_City,
    p.state = row.Rndrng_Prvdr_State_Abrvtn,
    p.gap_ratio = toFloat(row.Gap_Ratio),
    p.tot_srvcs = toInteger(row.Tot_Srvcs),
    p.avg_submitted_charge = toFloat(row.Avg_Sbmtd_Chrg);

-- Step 7: Relationships
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_NPI IS NOT NULL 
AND row.Rndrng_Prvdr_State_Abrvtn IS NOT NULL
MATCH (p:Provider {npi: row.Rndrng_NPI})
MATCH (s:State {abbr: row.Rndrng_Prvdr_State_Abrvtn})
MERGE (p)-[:LOCATED_IN_STATE]->(s);

LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_NPI IS NOT NULL 
AND row.HCPCS_Cd IS NOT NULL
MATCH (p:Provider {npi: row.Rndrng_NPI})
MATCH (s:Service {hcpcs_cd: row.HCPCS_Cd})
MERGE (p)-[r:PERFORMS]->(s)
SET r.tot_srvcs = toInteger(row.Tot_Srvcs),
    r.avg_submitted_charge = toFloat(row.Avg_Sbmtd_Chrg),
    r.avg_payment = toFloat(row.Avg_Mdcr_Pymt_Amt),
    r.gap_ratio = toFloat(row.Gap_Ratio);

LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_NPI IS NOT NULL 
AND row.Rndrng_Prvdr_Type IS NOT NULL
MATCH (p:Provider {npi: row.Rndrng_NPI})
MATCH (pt:ProviderType {name: row.Rndrng_Prvdr_Type})
MERGE (p)-[:HAS_TYPE]->(pt);

LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_NPI IS NOT NULL 
AND row.Rndrng_Prvdr_City IS NOT NULL
MATCH (p:Provider {npi: row.Rndrng_NPI})
MATCH (c:City {name: row.Rndrng_Prvdr_City, state: row.Rndrng_Prvdr_State_Abrvtn})
MERGE (p)-[:LOCATED_IN_CITY]->(c);

LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/SaranyaVaitheeswaran/MedicareClaim/refs/heads/main/medical_final_40k_stratified_anomaly.csv' AS row
WITH row WHERE row.Rndrng_Prvdr_City IS NOT NULL 
AND row.Rndrng_Prvdr_State_Abrvtn IS NOT NULL
MATCH (c:City {name: row.Rndrng_Prvdr_City, state: row.Rndrng_Prvdr_State_Abrvtn})
MATCH (s:State {abbr: row.Rndrng_Prvdr_State_Abrvtn})
MERGE (c)-[:IN_STATE]->(s);
