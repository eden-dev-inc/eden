use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, pg_query, redis_get, redis_hgetall, redis_zrevrange,
    weaviate_graphql,
};

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "pg_patient_records" => vec![
            (
                "Patient demographics",
                pg_query(
                    "SELECT gender, race, COUNT(*) as patients \
                     FROM patients GROUP BY gender, race ORDER BY patients DESC LIMIT 20",
                ),
            ),
            (
                "Encounter volume by class",
                pg_query(
                    "SELECT encounterclass, COUNT(*) as encounters, \
                     ROUND(AVG(total_claim_cost)::numeric, 2) as avg_cost \
                     FROM encounters GROUP BY encounterclass ORDER BY encounters DESC",
                ),
            ),
            (
                "Age distribution by decade",
                pg_query(
                    "SELECT (EXTRACT(YEAR FROM AGE(NOW(), birthdate::date))::int / 10) * 10 as age_decade, \
                     gender, COUNT(*) as patients \
                     FROM patients WHERE deathdate IS NULL \
                     GROUP BY age_decade, gender ORDER BY age_decade",
                ),
            ),
            (
                "Top states by patient count",
                pg_query(
                    "SELECT state, COUNT(*) as patients, \
                     ROUND(AVG(healthcare_expenses)::numeric, 2) as avg_expenses, \
                     ROUND(AVG(income)::numeric, 2) as avg_income \
                     FROM patients GROUP BY state ORDER BY patients DESC LIMIT 15",
                ),
            ),
            (
                "High-cost patients — top 20",
                pg_query(
                    "SELECT id, first, last, gender, \
                     ROUND(healthcare_expenses::numeric, 2) as expenses, \
                     ROUND(healthcare_coverage::numeric, 2) as coverage, \
                     ROUND((healthcare_expenses - healthcare_coverage)::numeric, 2) as out_of_pocket \
                     FROM patients ORDER BY healthcare_expenses DESC LIMIT 20",
                ),
            ),
            (
                "Mortality analysis by gender and ethnicity",
                pg_query(
                    "SELECT gender, ethnicity, COUNT(*) as deceased, \
                     ROUND(AVG(EXTRACT(YEAR FROM AGE(deathdate::date, birthdate::date)))::numeric, 1) as avg_age_at_death \
                     FROM patients WHERE deathdate IS NOT NULL \
                     GROUP BY gender, ethnicity ORDER BY deceased DESC",
                ),
            ),
            (
                "Coverage gap — patients with expenses far exceeding coverage",
                pg_query(
                    "SELECT id, first, last, \
                     ROUND(healthcare_expenses::numeric, 2) as expenses, \
                     ROUND(healthcare_coverage::numeric, 2) as coverage, \
                     ROUND((healthcare_expenses - healthcare_coverage)::numeric, 2) as gap \
                     FROM patients WHERE healthcare_expenses > healthcare_coverage * 2 \
                     ORDER BY (healthcare_expenses - healthcare_coverage) DESC LIMIT 20",
                ),
            ),
        ],
        "pg_billing" => vec![
            (
                "Claims by payer",
                pg_query(
                    "SELECT payer, COUNT(*) as claims, \
                     ROUND(SUM(total_claim_cost)::numeric, 2) as total_cost \
                     FROM encounters GROUP BY payer ORDER BY total_cost DESC LIMIT 20",
                ),
            ),
            (
                "Payer comparison — coverage vs uncovered amounts",
                pg_query(
                    "SELECT name, \
                     ROUND(amount_covered::numeric, 2) as covered, \
                     ROUND(amount_uncovered::numeric, 2) as uncovered, \
                     ROUND(revenue::numeric, 2) as revenue, \
                     unique_customers, ROUND(qols_avg::numeric, 2) as avg_qol \
                     FROM payers ORDER BY revenue DESC",
                ),
            ),
            (
                "Encounter cost by class with payer coverage",
                pg_query(
                    "SELECT encounterclass, \
                     COUNT(*) as encounters, \
                     ROUND(AVG(total_claim_cost)::numeric, 2) as avg_claim, \
                     ROUND(AVG(payer_coverage)::numeric, 2) as avg_payer_coverage, \
                     ROUND(AVG(total_claim_cost - payer_coverage)::numeric, 2) as avg_patient_responsibility \
                     FROM encounters GROUP BY encounterclass ORDER BY avg_claim DESC",
                ),
            ),
            (
                "Payers with highest uncovered encounter ratio",
                pg_query(
                    "SELECT name, covered_encounters, uncovered_encounters, \
                     ROUND(uncovered_encounters::numeric / NULLIF(covered_encounters + uncovered_encounters, 0) * 100, 1) as pct_uncovered, \
                     ROUND(amount_uncovered::numeric, 2) as total_uncovered \
                     FROM payers WHERE covered_encounters + uncovered_encounters > 0 \
                     ORDER BY pct_uncovered DESC",
                ),
            ),
            (
                "Top payers by covered medications",
                pg_query(
                    "SELECT name, covered_medications, uncovered_medications, \
                     ROUND(revenue::numeric, 2) as revenue, member_months \
                     FROM payers ORDER BY covered_medications DESC LIMIT 15",
                ),
            ),
            (
                "High-cost encounters with low payer coverage",
                pg_query(
                    "SELECT id, encounterclass, \
                     ROUND(total_claim_cost::numeric, 2) as claim, \
                     ROUND(payer_coverage::numeric, 2) as covered, \
                     ROUND((total_claim_cost - payer_coverage)::numeric, 2) as patient_owes \
                     FROM encounters WHERE total_claim_cost > 1000 AND payer_coverage < total_claim_cost * 0.3 \
                     ORDER BY total_claim_cost DESC LIMIT 20",
                ),
            ),
        ],
        "pg_cms_claims" => vec![
            (
                "CMS beneficiary demographics",
                pg_query(
                    "SELECT BENE_SEX_IDENT_CD as sex, BENE_RACE_CD as race, \
                     COUNT(*) as beneficiaries, \
                     ROUND(AVG(MEDREIMB_IP + MEDREIMB_OP + MEDREIMB_CAR)::numeric, 2) as avg_total_reimbursement \
                     FROM bene_summary GROUP BY BENE_SEX_IDENT_CD, BENE_RACE_CD \
                     ORDER BY beneficiaries DESC LIMIT 20",
                ),
            ),
            (
                "Top inpatient claims by payment amount",
                pg_query(
                    "SELECT CLM_ID, DESYNPUF_ID, CLM_PMT_AMT, \
                     CLM_ADMSN_DT, NCH_BENE_DSCHRG_DT, CLM_DRG_CD, \
                     CLM_UTLZTN_DAY_CNT as days_utilized \
                     FROM ip_claims ORDER BY CLM_PMT_AMT DESC LIMIT 20",
                ),
            ),
            (
                "Outpatient claims by provider",
                pg_query(
                    "SELECT PRVDR_NUM, COUNT(*) as claims, \
                     ROUND(SUM(CLM_PMT_AMT)::numeric, 2) as total_paid, \
                     ROUND(AVG(CLM_PMT_AMT)::numeric, 2) as avg_paid \
                     FROM op_claims GROUP BY PRVDR_NUM \
                     ORDER BY total_paid DESC LIMIT 20",
                ),
            ),
            (
                "Prescription drug spending by beneficiary",
                pg_query(
                    "SELECT DESYNPUF_ID, COUNT(*) as prescriptions, \
                     ROUND(SUM(TOT_RX_CST_AMT)::numeric, 2) as total_cost, \
                     ROUND(SUM(PTNT_PAY_AMT)::numeric, 2) as patient_paid \
                     FROM pde GROUP BY DESYNPUF_ID \
                     ORDER BY total_cost DESC LIMIT 20",
                ),
            ),
            (
                "Chronic condition prevalence",
                pg_query(
                    "SELECT \
                     SUM(CASE WHEN SP_DIABETES = 1 THEN 1 ELSE 0 END) as diabetes, \
                     SUM(CASE WHEN SP_CHF = 1 THEN 1 ELSE 0 END) as heart_failure, \
                     SUM(CASE WHEN SP_COPD = 1 THEN 1 ELSE 0 END) as copd, \
                     SUM(CASE WHEN SP_DEPRESSN = 1 THEN 1 ELSE 0 END) as depression, \
                     SUM(CASE WHEN SP_ALZHDMTA = 1 THEN 1 ELSE 0 END) as alzheimers, \
                     SUM(CASE WHEN SP_CNCR = 1 THEN 1 ELSE 0 END) as cancer, \
                     COUNT(*) as total_beneficiaries \
                     FROM bene_summary",
                ),
            ),
            (
                "Carrier claims — top diagnosis codes",
                pg_query(
                    "SELECT ICD9_DGNS_CD_1 as diagnosis, COUNT(*) as claims, \
                     ROUND(SUM(LINE_NCH_PMT_AMT_1)::numeric, 2) as total_paid \
                     FROM car_claims WHERE ICD9_DGNS_CD_1 IS NOT NULL \
                     GROUP BY ICD9_DGNS_CD_1 ORDER BY claims DESC LIMIT 20",
                ),
            ),
        ],
        "mongo_clinical_docs" => vec![
            (
                "Conditions by frequency",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$description", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Top diagnoses by unique patient count",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$description", "patients": {"$addToSet": "$patient"}, "total": {"$sum": 1}}},
                        {"$project": {"description": "$_id", "total": 1, "unique_patients": {"$size": "$patients"}}},
                        {"$sort": {"unique_patients": -1}},
                        {"$limit": 15}
                    ]),
                ),
            ),
            (
                "Medication frequency — most prescribed",
                mongo_aggregate(
                    "clinical",
                    "medications",
                    json!([
                        {"$group": {"_id": "$description", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Condition co-occurrence — patients with 3+ distinct conditions",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$patient", "conditions": {"$addToSet": "$description"}}},
                        {"$project": {"patient": "$_id", "num_conditions": {"$size": "$conditions"}, "conditions": 1}},
                        {"$match": {"num_conditions": {"$gte": 3}}},
                        {"$sort": {"num_conditions": -1}},
                        {"$limit": 15}
                    ]),
                ),
            ),
            (
                "Medications per patient — polypharmacy detection",
                mongo_aggregate(
                    "clinical",
                    "medications",
                    json!([
                        {"$group": {"_id": "$patient", "medications": {"$addToSet": "$description"}}},
                        {"$project": {"patient": "$_id", "med_count": {"$size": "$medications"}, "medications": 1}},
                        {"$match": {"med_count": {"$gte": 4}}},
                        {"$sort": {"med_count": -1}},
                        {"$limit": 15}
                    ]),
                ),
            ),
            (
                "Conditions per encounter — diagnostic intensity",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$encounter", "condition_count": {"$sum": 1}, "conditions": {"$push": "$description"}}},
                        {"$sort": {"condition_count": -1}},
                        {"$limit": 15}
                    ]),
                ),
            ),
        ],
        "mongo_lab_results" => vec![
            (
                "Observation types",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$group": {"_id": "$description", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Vital signs — blood pressure readings distribution",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$match": {"description": {"$regex": "Blood Pressure"}}},
                        {"$group": {"_id": "$description", "count": {"$sum": 1},
                                    "avg_value": {"$avg": {"$toDouble": "$value"}}}},
                        {"$sort": {"count": -1}}
                    ]),
                ),
            ),
            (
                "Abnormal lab values — observations with extreme readings",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$match": {"type": "numeric"}},
                        {"$group": {"_id": "$description", "count": {"$sum": 1},
                                    "min_val": {"$min": {"$toDouble": "$value"}},
                                    "max_val": {"$max": {"$toDouble": "$value"}},
                                    "avg_val": {"$avg": {"$toDouble": "$value"}}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Lab categories breakdown",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$group": {"_id": "$category", "count": {"$sum": 1},
                                    "unique_tests": {"$addToSet": "$description"}}},
                        {"$project": {"category": "$_id", "count": 1,
                                      "num_test_types": {"$size": "$unique_tests"}}},
                        {"$sort": {"count": -1}}
                    ]),
                ),
            ),
            (
                "Patients with most lab observations",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$group": {"_id": "$patient", "obs_count": {"$sum": 1},
                                    "categories": {"$addToSet": "$category"}}},
                        {"$project": {"patient": "$_id", "obs_count": 1,
                                      "num_categories": {"$size": "$categories"}}},
                        {"$sort": {"obs_count": -1}},
                        {"$limit": 15}
                    ]),
                ),
            ),
            (
                "Glucose and cholesterol readings summary",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$match": {"description": {"$regex": "Glucose|Cholesterol", "$options": "i"}}},
                        {"$group": {"_id": "$description", "count": {"$sum": 1},
                                    "avg_value": {"$avg": {"$toDouble": "$value"}},
                                    "units": {"$first": "$units"}}},
                        {"$sort": {"count": -1}}
                    ]),
                ),
            ),
        ],
        "ch_billing_analytics" => vec![
            (
                "Daily encounter volume",
                ch_query(
                    "SELECT event_day, count() as encounters, \
                     round(sum(total_claim_cost), 2) as total_cost \
                     FROM analytics.encounter_events GROUP BY event_day ORDER BY event_day DESC LIMIT 30",
                ),
            ),
            (
                "Monthly cost trends",
                ch_query(
                    "SELECT toStartOfMonth(event_day) as month, \
                     count() as encounters, \
                     round(sum(total_claim_cost), 2) as total_cost, \
                     round(avg(total_claim_cost), 2) as avg_cost \
                     FROM analytics.encounter_events GROUP BY month ORDER BY month DESC LIMIT 24",
                ),
            ),
            (
                "Encounter class cost distribution",
                ch_query(
                    "SELECT encounterclass, count() as encounters, \
                     round(min(total_claim_cost), 2) as min_cost, \
                     round(avg(total_claim_cost), 2) as avg_cost, \
                     round(max(total_claim_cost), 2) as max_cost, \
                     round(sum(total_claim_cost), 2) as total_cost \
                     FROM analytics.encounter_events GROUP BY encounterclass ORDER BY total_cost DESC",
                ),
            ),
            (
                "Payer coverage ratio by encounter class",
                ch_query(
                    "SELECT encounterclass, \
                     round(avg(payer_coverage), 2) as avg_payer_coverage, \
                     round(avg(total_claim_cost), 2) as avg_claim, \
                     round(avg(payer_coverage) / nullIf(avg(total_claim_cost), 0) * 100, 1) as pct_covered \
                     FROM analytics.encounter_events GROUP BY encounterclass ORDER BY pct_covered DESC",
                ),
            ),
            (
                "Day-of-week encounter patterns",
                ch_query(
                    "SELECT toDayOfWeek(event_day) as dow, \
                     count() as encounters, \
                     round(avg(total_claim_cost), 2) as avg_cost \
                     FROM analytics.encounter_events GROUP BY dow ORDER BY dow",
                ),
            ),
            (
                "High-cost days — days exceeding $50k total claims",
                ch_query(
                    "SELECT event_day, count() as encounters, \
                     round(sum(total_claim_cost), 2) as total_cost \
                     FROM analytics.encounter_events \
                     GROUP BY event_day HAVING sum(total_claim_cost) > 50000 \
                     ORDER BY total_cost DESC LIMIT 20",
                ),
            ),
        ],
        "redis_alerts" => vec![
            ("Total patients", redis_get("stats:total_patients")),
            (
                "Bed availability — Emergency",
                redis_hgetall("beds:Emergency"),
            ),
            ("Bed availability — ICU", redis_hgetall("beds:ICU")),
            ("Total encounters", redis_get("stats:total_encounters")),
            (
                "Bed availability — General Ward",
                redis_hgetall("beds:General"),
            ),
            (
                "Critical lab alerts — top 10 by severity",
                redis_zrevrange("alerts:critical_labs", 0, 9),
            ),
            (
                "High-priority patient alerts — top 10",
                redis_zrevrange("alerts:patient_priority", 0, 9),
            ),
            (
                "Bed utilization — Surgery department",
                redis_hgetall("beds:Surgery"),
            ),
        ],
        "weaviate_clinical" => vec![
            (
                "Search: diabetes conditions",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"diabetes glucose insulin\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
            (
                "Search: heart disease and cardiovascular conditions",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"heart disease coronary artery cardiac arrest atrial fibrillation\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
            (
                "Search: cancer and oncology conditions",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"cancer tumor malignant neoplasm carcinoma lymphoma\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
            (
                "Search: respiratory conditions",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"asthma COPD pneumonia bronchitis respiratory failure\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
            (
                "Search: mental health conditions",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"depression anxiety disorder bipolar schizophrenia PTSD\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
            (
                "Search: chronic pain and musculoskeletal",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"chronic pain arthritis osteoporosis fibromyalgia back pain\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
        ],
        _ => vec![],
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    vec![
        // Cross-silo: Patient risk stratification
        // Correlation key: patient (UUID) — shared across PG patients, encounters, Mongo conditions, CH analytics
        vec![
            (
                "pg_patient_records",
                "High-cost patients with encounter details",
                pg_query(
                    "SELECT p.id as patient_id, p.gender, p.race, \
                     p.healthcare_expenses, COUNT(e.id) as encounter_count, \
                     ROUND(SUM(e.total_claim_cost)::numeric, 2) as total_claims \
                     FROM patients p JOIN encounters e ON p.id = e.patient \
                     GROUP BY p.id, p.gender, p.race, p.healthcare_expenses \
                     HAVING COUNT(e.id) > 5 \
                     ORDER BY total_claims DESC LIMIT 20",
                ),
            ),
            (
                "mongo_clinical_docs",
                "Chronic conditions for high-cost patients (2+ conditions per patient)",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$patient", "condition_count": {"$sum": 1},
                                    "conditions": {"$push": "$description"}}},
                        {"$match": {"condition_count": {"$gte": 3}}},
                        {"$sort": {"condition_count": -1}},
                        {"$limit": 20}
                    ]),
                ),
            ),
            (
                "mongo_lab_results",
                "Lab observation volume for multi-condition patients",
                mongo_aggregate(
                    "laboratory",
                    "observations",
                    json!([
                        {"$group": {"_id": "$patient", "obs_count": {"$sum": 1},
                                    "categories": {"$addToSet": "$category"}}},
                        {"$sort": {"obs_count": -1}},
                        {"$limit": 20}
                    ]),
                ),
            ),
        ],
        // Cross-silo: Encounter cost analysis across billing + clinical + analytics
        // Correlation key: encounter ID and patient ID
        vec![
            (
                "pg_patient_records",
                "Costliest encounter types with patient demographics",
                pg_query(
                    "SELECT e.encounterclass, p.gender, \
                     COUNT(*) as encounters, \
                     ROUND(AVG(e.total_claim_cost)::numeric, 2) as avg_cost, \
                     ROUND(SUM(e.total_claim_cost)::numeric, 2) as total_cost \
                     FROM encounters e JOIN patients p ON e.patient = p.id \
                     GROUP BY e.encounterclass, p.gender \
                     ORDER BY total_cost DESC LIMIT 20",
                ),
            ),
            (
                "ch_billing_analytics",
                "Daily cost trends for high-cost encounter types",
                ch_query(
                    "SELECT event_day, encounterclass, \
                     count() as encounters, \
                     round(sum(total_claim_cost), 2) as daily_cost, \
                     round(avg(total_claim_cost), 2) as avg_cost \
                     FROM analytics.encounter_events \
                     GROUP BY event_day, encounterclass \
                     ORDER BY daily_cost DESC LIMIT 30",
                ),
            ),
            (
                "redis_alerts",
                "Current bed availability vs demand",
                redis_hgetall("beds:Emergency"),
            ),
        ],
        // Cross-silo: Medication + condition correlation
        // Correlation key: patient ID across Mongo conditions and medications
        vec![
            (
                "mongo_clinical_docs",
                "Most prescribed conditions (by patient volume)",
                mongo_aggregate(
                    "clinical",
                    "conditions",
                    json!([
                        {"$group": {"_id": "$description", "patients": {"$addToSet": "$patient"},
                                    "count": {"$sum": 1}}},
                        {"$project": {"description": "$_id", "count": 1,
                                      "unique_patients": {"$size": "$patients"}}},
                        {"$sort": {"unique_patients": -1}},
                        {"$limit": 10}
                    ]),
                ),
            ),
            (
                "mongo_clinical_docs",
                "Medications prescribed for top conditions",
                mongo_aggregate(
                    "clinical",
                    "medications",
                    json!([
                        {"$group": {"_id": "$description", "patients": {"$addToSet": "$patient"},
                                    "count": {"$sum": 1}}},
                        {"$project": {"medication": "$_id", "count": 1,
                                      "unique_patients": {"$size": "$patients"}}},
                        {"$sort": {"unique_patients": -1}},
                        {"$limit": 10}
                    ]),
                ),
            ),
            (
                "weaviate_clinical",
                "Similar conditions in clinical knowledge base",
                weaviate_graphql(
                    "{ Get { ClinicalCondition(nearText: {concepts: [\"chronic disease diabetes hypertension heart failure\"]}, limit: 10) { patient_id description } } }",
                ),
            ),
        ],
    ]
}
