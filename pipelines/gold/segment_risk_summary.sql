CREATE OR REPLACE MATERIALIZED VIEW ${home_credit.gold_schema}.segment_risk_summary AS
WITH overall AS (
    SELECT COUNT(*) AS total_customers,
           AVG(target) AS overall_default_rate
    FROM ${home_credit.gold_schema}.customer_risk_profile
),
by_segment AS (
    SELECT segment,
           COUNT(*) AS customers,
           SUM(target) AS no_of_defaults,
           AVG(target) AS default_rate
    FROM ${home_credit.gold_schema}.customer_risk_profile
    GROUP BY segment
)
SELECT s.segment,
       s.customers,
       s.no_of_defaults,
       ROUND(s.default_rate, 4) AS default_rate,
       ROUND(o.overall_default_rate, 4) AS overall_default_rate,
       ROUND(s.default_rate / o.overall_default_rate, 2) AS lift,
       ROUND(s.customers / o.total_customers, 4) AS pct_of_customers
FROM by_segment s
CROSS JOIN overall o;