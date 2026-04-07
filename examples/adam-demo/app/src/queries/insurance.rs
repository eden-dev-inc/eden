use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, pg_query, redis_get, redis_hgetall, redis_smembers,
    redis_zrevrange, weaviate_graphql,
};

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "pg_policy_admin" => vec![
            (
                "Policy count by area",
                pg_query(
                    "SELECT area, COUNT(*) as policies, \
                     ROUND(AVG(exposure)::numeric, 4) as avg_exposure \
                     FROM policies GROUP BY area ORDER BY policies DESC",
                ),
            ),
            (
                "Claims by vehicle brand",
                pg_query(
                    "SELECT veh_brand, COUNT(*) as policies, \
                     SUM(claim_nb) as total_claims \
                     FROM policies GROUP BY veh_brand ORDER BY total_claims DESC LIMIT 20",
                ),
            ),
            (
                "Exposure analysis by region",
                pg_query(
                    "SELECT region, COUNT(*) as policies, \
                     ROUND(AVG(exposure)::numeric, 4) as avg_exposure, \
                     ROUND(MIN(exposure)::numeric, 4) as min_exposure, \
                     ROUND(MAX(exposure)::numeric, 4) as max_exposure \
                     FROM policies GROUP BY region ORDER BY avg_exposure DESC LIMIT 20",
                ),
            ),
            (
                "Bonus-malus distribution",
                pg_query(
                    "SELECT bonus_malus, COUNT(*) as policies, \
                     SUM(claim_nb) as total_claims, \
                     ROUND(AVG(exposure)::numeric, 4) as avg_exposure \
                     FROM policies GROUP BY bonus_malus ORDER BY bonus_malus",
                ),
            ),
            (
                "Vehicle age vs claim frequency",
                pg_query(
                    "SELECT veh_age, COUNT(*) as policies, \
                     SUM(claim_nb) as total_claims, \
                     ROUND(SUM(claim_nb)::numeric / NULLIF(COUNT(*), 0)::numeric, 4) as claim_rate \
                     FROM policies GROUP BY veh_age ORDER BY veh_age",
                ),
            ),
            (
                "Young driver risk profile",
                pg_query(
                    "SELECT driv_age, veh_power, COUNT(*) as policies, \
                     SUM(claim_nb) as claims, \
                     ROUND(SUM(claim_nb)::numeric / NULLIF(COUNT(*), 0)::numeric, 4) as claim_rate, \
                     ROUND(AVG(bonus_malus)::numeric, 1) as avg_bonus_malus \
                     FROM policies WHERE driv_age <= 25 \
                     GROUP BY driv_age, veh_power \
                     ORDER BY claim_rate DESC LIMIT 20",
                ),
            ),
            (
                "High-density urban claim rates",
                pg_query(
                    "SELECT us_state, area, \
                     COUNT(*) as policies, SUM(claim_nb) as claims, \
                     ROUND(AVG(density)::numeric, 0) as avg_density, \
                     ROUND(SUM(claim_nb)::numeric / NULLIF(COUNT(*), 0)::numeric, 4) as claim_rate \
                     FROM policies WHERE density > 5000 \
                     GROUP BY us_state, area ORDER BY claim_rate DESC LIMIT 20",
                ),
            ),
            (
                "Top claim amounts by policy",
                pg_query(
                    "SELECT p.id_pol, p.us_state, p.veh_brand, p.driv_age, \
                     c.claim_amount, p.bonus_malus \
                     FROM claims c JOIN policies p ON c.id_pol = p.id_pol \
                     ORDER BY c.claim_amount DESC LIMIT 20",
                ),
            ),
        ],
        "pg_risk_scoring" => vec![
            (
                "Risk feature distribution",
                pg_query(
                    "SELECT target, COUNT(*) as records \
                     FROM driver_risk GROUP BY target ORDER BY target",
                ),
            ),
            (
                "Car feature importance proxy (avg by claim outcome)",
                pg_query(
                    "SELECT target, \
                     ROUND(AVG(ps_car_01)::numeric, 4) as avg_car_01, \
                     ROUND(AVG(ps_car_06)::numeric, 4) as avg_car_06, \
                     ROUND(AVG(ps_car_11)::numeric, 4) as avg_car_11, \
                     ROUND(AVG(ps_car_13)::numeric, 4) as avg_car_13, \
                     ROUND(AVG(ps_car_15)::numeric, 4) as avg_car_15 \
                     FROM driver_risk GROUP BY target ORDER BY target",
                ),
            ),
            (
                "Individual feature averages by claim outcome",
                pg_query(
                    "SELECT target, \
                     ROUND(AVG(ps_ind_01)::numeric, 4) as avg_ind_01, \
                     ROUND(AVG(ps_ind_03)::numeric, 4) as avg_ind_03, \
                     ROUND(AVG(ps_ind_06)::numeric, 4) as avg_ind_06, \
                     ROUND(AVG(ps_ind_15)::numeric, 4) as avg_ind_15 \
                     FROM driver_risk GROUP BY target ORDER BY target",
                ),
            ),
            (
                "Claim prediction distribution by registration region",
                pg_query(
                    "SELECT ROUND(ps_reg_01::numeric, 1) as reg_bucket, \
                     target, COUNT(*) as drivers \
                     FROM driver_risk GROUP BY reg_bucket, target \
                     ORDER BY reg_bucket, target",
                ),
            ),
            (
                "High-risk driver calculated feature spread",
                pg_query(
                    "SELECT \
                     ROUND(AVG(ps_calc_01)::numeric, 4) as avg_calc_01, \
                     ROUND(AVG(ps_calc_04)::numeric, 4) as avg_calc_04, \
                     ROUND(AVG(ps_calc_10)::numeric, 4) as avg_calc_10, \
                     ROUND(AVG(ps_calc_14)::numeric, 4) as avg_calc_14, \
                     COUNT(*) as high_risk_drivers \
                     FROM driver_risk WHERE target = 1",
                ),
            ),
            (
                "Registration vs car feature correlation",
                pg_query(
                    "SELECT ROUND(ps_reg_02::numeric, 1) as reg_02_bucket, \
                     ROUND(AVG(ps_car_13)::numeric, 4) as avg_car_13, \
                     ROUND(AVG(ps_car_15)::numeric, 4) as avg_car_15, \
                     COUNT(*) as drivers, \
                     ROUND(AVG(target)::numeric, 4) as claim_rate \
                     FROM driver_risk GROUP BY reg_02_bucket \
                     ORDER BY claim_rate DESC LIMIT 15",
                ),
            ),
        ],
        "ch_claims_analytics" => vec![
            (
                "Accident severity distribution",
                ch_query(
                    "SELECT severity, count() as incidents, \
                     round(avg(distance_mi), 2) as avg_distance \
                     FROM analytics.accidents GROUP BY severity ORDER BY incidents DESC",
                ),
            ),
            (
                "Accidents by weather condition",
                ch_query(
                    "SELECT weather_condition, count() as incidents \
                     FROM analytics.accidents WHERE weather_condition != '' \
                     GROUP BY weather_condition ORDER BY incidents DESC LIMIT 20",
                ),
            ),
            (
                "Geographic hotspots by state and city",
                ch_query(
                    "SELECT state, city, count() as incidents, \
                     round(avg(severity), 2) as avg_severity \
                     FROM analytics.accidents \
                     GROUP BY state, city ORDER BY incidents DESC LIMIT 25",
                ),
            ),
            (
                "Temporal trend: accidents by hour of day",
                ch_query(
                    "SELECT toHour(start_time) as hour_of_day, \
                     count() as incidents, \
                     round(avg(severity), 2) as avg_severity, \
                     round(avg(distance_mi), 2) as avg_distance \
                     FROM analytics.accidents \
                     GROUP BY hour_of_day ORDER BY hour_of_day",
                ),
            ),
            (
                "Weather impact on severity",
                ch_query(
                    "SELECT weather_condition, severity, \
                     count() as incidents, \
                     round(avg(visibility_mi), 2) as avg_visibility, \
                     round(avg(wind_speed_mph), 2) as avg_wind_speed \
                     FROM analytics.accidents WHERE weather_condition != '' \
                     GROUP BY weather_condition, severity \
                     ORDER BY incidents DESC LIMIT 30",
                ),
            ),
            (
                "Severity by time of day (day vs night)",
                ch_query(
                    "SELECT sunrise_sunset, severity, \
                     count() as incidents, \
                     round(avg(distance_mi), 2) as avg_distance, \
                     round(avg(temperature_f), 1) as avg_temp \
                     FROM analytics.accidents WHERE sunrise_sunset != '' \
                     GROUP BY sunrise_sunset, severity \
                     ORDER BY sunrise_sunset, severity",
                ),
            ),
            (
                "Daily severity summary trend",
                ch_query(
                    "SELECT * FROM analytics.daily_severity \
                     ORDER BY day DESC LIMIT 30",
                ),
            ),
            (
                "Junction and crossing accident rates",
                ch_query(
                    "SELECT junction, crossing, traffic_signal, \
                     count() as incidents, \
                     round(avg(severity), 2) as avg_severity \
                     FROM analytics.accidents \
                     GROUP BY junction, crossing, traffic_signal \
                     ORDER BY incidents DESC",
                ),
            ),
        ],
        "mongo_incidents" => vec![
            (
                "Accidents by state",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$group": {"_id": "$State", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Accidents by time of day",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$group": {"_id": "$Sunrise_Sunset", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ]),
                ),
            ),
            (
                "Worst intersections by severity",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$match": {"Junction": true, "Severity": {"$gte": 3}}},
                        {"$group": {"_id": {"city": "$City", "state": "$State"},
                                    "count": {"$sum": 1},
                                    "avg_severity": {"$avg": "$Severity"}}},
                        {"$sort": {"count": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Highway vs urban accidents",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$bucket": {"groupBy": "$Distance", "boundaries": [0, 0.5, 2, 5, 50, 500],
                                     "default": "Other",
                                     "output": {"count": {"$sum": 1},
                                                "avg_severity": {"$avg": "$Severity"}}}},
                        {"$sort": {"_id": 1}}
                    ]),
                ),
            ),
            (
                "Weather-related accident clustering",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$match": {"Weather_Condition": {"$in": ["Rain", "Snow", "Fog", "Hail", "Freezing Rain"]}}},
                        {"$group": {"_id": {"weather": "$Weather_Condition", "state": "$State"},
                                    "count": {"$sum": 1},
                                    "avg_visibility": {"$avg": "$Visibility"}}},
                        {"$sort": {"count": -1}}, {"$limit": 25}
                    ]),
                ),
            ),
            (
                "Severe nighttime accidents with poor visibility",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$match": {"Sunrise_Sunset": "Night", "Visibility": {"$lt": 5}, "Severity": {"$gte": 3}}},
                        {"$group": {"_id": "$State", "count": {"$sum": 1},
                                    "avg_visibility": {"$avg": "$Visibility"},
                                    "avg_severity": {"$avg": "$Severity"}}},
                        {"$sort": {"count": -1}}, {"$limit": 15}
                    ]),
                ),
            ),
            (
                "Traffic signal vs no-signal accident severity",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$group": {"_id": {"signal": "$Traffic_Signal", "crossing": "$Crossing"},
                                    "count": {"$sum": 1},
                                    "avg_severity": {"$avg": "$Severity"}}},
                        {"$sort": {"count": -1}}
                    ]),
                ),
            ),
        ],
        "redis_claims" => vec![
            ("Active policies count", redis_get("stats:active_policies")),
            ("Open claims count", redis_get("stats:open_claims")),
            ("Average claim amount", redis_get("stats:avg_claim_amount")),
            ("Current loss ratio", redis_get("stats:loss_ratio")),
            (
                "Agent resolution leaderboard (top 10)",
                redis_zrevrange("leaderboard:agent_resolutions", 0, 9),
            ),
            (
                "Policy status breakdown for policy #1",
                redis_hgetall("policy:1"),
            ),
            (
                "Claim pipeline: claims for policy #1",
                redis_smembers("policy_claims:1"),
            ),
            (
                "State policy count (California)",
                redis_get("state_policies:CA"),
            ),
        ],
        "weaviate_claims" => vec![
            (
                "Search: highway multi-vehicle",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"highway multi-vehicle collision severe\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: weather-related",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"rain fog snow ice weather\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: rear-end collision",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"rear-end collision tailgating stopped traffic\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: pedestrian involved",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"pedestrian hit crosswalk injury\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: DUI alcohol-related",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"DUI drunk driving alcohol impaired\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: construction zone",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"construction zone road work lane closure\"]}, limit: 10) { state severity description } } }",
                ),
            ),
            (
                "Search: intersection T-bone crash",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"intersection T-bone side impact red light\"]}, limit: 10) { state severity description } } }",
                ),
            ),
        ],
        _ => vec![],
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    vec![
        // Cross-silo: Policy risk vs accident severity — joined on us_state <-> State
        // Correlation: policy claim frequency by US state (PG) -> accident severity by state (CH) -> state policy count (Redis)
        vec![
            (
                "pg_policy_admin",
                "High-claim-frequency policies by US state and vehicle",
                pg_query(
                    "SELECT us_state, veh_brand, COUNT(*) as policies, \
                     SUM(claim_nb) as total_claims, \
                     ROUND(AVG(exposure)::numeric, 4) as avg_exposure, \
                     ROUND(AVG(bonus_malus)::numeric, 1) as avg_bonus_malus \
                     FROM policies WHERE claim_nb > 0 \
                     GROUP BY us_state, veh_brand \
                     ORDER BY total_claims DESC LIMIT 20",
                ),
            ),
            (
                "ch_claims_analytics",
                "Accident severity by state and weather (matching policy states)",
                ch_query(
                    "SELECT state, weather_condition, severity, \
                     count() as incidents, \
                     round(avg(distance_mi), 2) as avg_distance \
                     FROM analytics.accidents \
                     WHERE weather_condition != '' \
                     GROUP BY state, weather_condition, severity \
                     ORDER BY incidents DESC LIMIT 20",
                ),
            ),
            (
                "redis_claims",
                "Policy count for top state (CA)",
                redis_get("state_policies:CA"),
            ),
        ],
        // Cross-silo: Claim investigation — trace claim to policy to accident
        // Correlation key: claim_id -> claim_policy:{id} (Redis) -> policy:{id} (Redis) -> accident details (Mongo)
        vec![
            (
                "redis_claims",
                "Claim status and assigned agent for claim #1",
                redis_hgetall("claim:1"),
            ),
            (
                "redis_claims",
                "Policy linked to claim #1",
                redis_get("claim_policy:1"),
            ),
            (
                "mongo_incidents",
                "Recent severe accidents with descriptions",
                mongo_aggregate(
                    "incidents",
                    "accidents",
                    json!([
                        {"$match": {"Severity": {"$gte": 3}}},
                        {"$project": {"State": 1, "City": 1, "Severity": 1,
                                      "Weather_Condition": 1, "Description": 1}},
                        {"$sort": {"Severity": -1}},
                        {"$limit": 10}
                    ]),
                ),
            ),
            (
                "weaviate_claims",
                "Similar historical accidents",
                weaviate_graphql(
                    "{ Get { AccidentReport(nearText: {concepts: [\"severe multi-vehicle highway collision\"]}, limit: 5) { state severity description } } }",
                ),
            ),
        ],
        // Cross-silo: Risk model validation — compare risk scores to actual claim rates
        // Correlation: predicted risk (PG risk_scoring) vs actual claims (PG policy_admin) vs loss ratio (Redis)
        vec![
            (
                "pg_risk_scoring",
                "Driver risk score distribution (predicted claim probability)",
                pg_query(
                    "SELECT target, COUNT(*) as drivers, \
                     ROUND(AVG(ps_car_11)::numeric, 2) as avg_car_age_proxy \
                     FROM driver_risk GROUP BY target ORDER BY target",
                ),
            ),
            (
                "pg_policy_admin",
                "Actual claim rates by driver age and vehicle power",
                pg_query(
                    "SELECT driv_age, veh_power, COUNT(*) as policies, \
                     SUM(claim_nb) as claims, \
                     ROUND(SUM(claim_nb)::numeric / COUNT(*)::numeric, 4) as claim_rate \
                     FROM policies \
                     GROUP BY driv_age, veh_power \
                     HAVING COUNT(*) > 50 \
                     ORDER BY claim_rate DESC LIMIT 20",
                ),
            ),
            (
                "redis_claims",
                "Current loss ratio",
                redis_get("stats:loss_ratio"),
            ),
        ],
    ]
}
