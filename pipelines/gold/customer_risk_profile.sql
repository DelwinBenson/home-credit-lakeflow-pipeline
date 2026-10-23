CREATE OR REPLACE MATERIALIZED VIEW ${home_credit.gold_schema}.customer_risk_profile AS
WITH bureau AS (
    SELECT sk_id_curr,
           COUNT(*) AS bureau_loan_count,
           SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_loan_count
    FROM ${home_credit.silver_schema}.bureau_loan
    GROUP BY sk_id_curr
),
prev AS (
    SELECT sk_id_curr,
           SUM(CASE WHEN is_refused THEN 1 ELSE 0 END) AS refused_count
    FROM ${home_credit.silver_schema}.previous_application
    GROUP BY sk_id_curr
),
cards AS (
    SELECT sk_id_curr,
           COUNT(*) AS card_month_count,
           MAX(sk_dpd) AS max_days_past_due,
           AVG(CASE WHEN amt_credit_limit_actual > 0
                    THEN amt_balance / amt_credit_limit_actual END) AS avg_utilization
    FROM ${home_credit.silver_schema}.credit_card_balance
    GROUP BY sk_id_curr
)
SELECT
    c.sk_id_curr,
    c.target,
    c.age_years,
    c.amt_income_total,
    c.amt_credit,
    COALESCE(p.refused_count, 0)      AS refused_count,
    COALESCE(b.bureau_loan_count, 0)  AS bureau_loan_count,
    COALESCE(b.active_loan_count, 0)  AS active_loan_count,
    b.sk_id_curr IS NOT NULL          AS has_bureau_history,
    COALESCE(k.card_month_count, 0)   AS card_month_count,
    k.max_days_past_due,
    ROUND(k.avg_utilization, 4)       AS avg_utilization,
    k.sk_id_curr IS NOT NULL          AS has_card_history,
    CASE WHEN COALESCE(p.refused_count, 0) >= 3 THEN 'high'
         WHEN COALESCE(p.refused_count, 0) >= 1 THEN 'medium'
         ELSE 'low' END              AS segment
FROM ${home_credit.silver_schema}.customer c
LEFT JOIN bureau b ON c.sk_id_curr = b.sk_id_curr
LEFT JOIN prev p   ON c.sk_id_curr = p.sk_id_curr
LEFT JOIN cards k  ON c.sk_id_curr = k.sk_id_curr;