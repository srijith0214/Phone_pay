-- ================================================================
-- PhonePe – 5 Business Case Study SQL Queries
-- ================================================================
-- BC1: Decoding Transaction Dynamics
-- BC2: Device Dominance & User Engagement Analysis
-- BC3: Insurance Penetration & Growth Potential
-- BC7: Transaction Analysis Across States & Districts
-- BC8: User Registration Analysis
-- ================================================================


-- ================================================================
-- BUSINESS CASE 1: Decoding Transaction Dynamics on PhonePe
-- ================================================================
-- Goal: Understand variations in transaction behavior across
--       states, quarters, and payment categories.

-- 1A: Aggregate transactions by category and quarter
SELECT
    year,
    quarter,
    transaction_type,
    SUM(transaction_count)                          AS total_count,
    SUM(transaction_amount)                         AS total_amount,
    ROUND(SUM(transaction_amount)
          / NULLIF(SUM(transaction_count), 0), 2)  AS avg_txn_value
FROM Aggregated_transaction
GROUP BY year, quarter, transaction_type
ORDER BY year, quarter, total_amount DESC;


-- 1B: Quarter-on-Quarter growth per transaction category
WITH quarterly AS (
    SELECT
        year, quarter, transaction_type,
        SUM(transaction_count)  AS total_count,
        SUM(transaction_amount) AS total_amount
    FROM Aggregated_transaction
    GROUP BY year, quarter, transaction_type
),
with_lag AS (
    SELECT *,
        LAG(total_amount) OVER (
            PARTITION BY transaction_type
            ORDER BY year, quarter
        ) AS prev_amount,
        LAG(total_count)  OVER (
            PARTITION BY transaction_type
            ORDER BY year, quarter
        ) AS prev_count
    FROM quarterly
)
SELECT
    year,
    quarter,
    transaction_type,
    total_amount,
    total_count,
    prev_amount,
    ROUND((total_amount - prev_amount) / NULLIF(prev_amount, 0) * 100, 2) AS qoq_growth_pct
FROM with_lag
ORDER BY year, quarter, transaction_type;


-- 1C: State-level performance vs national average (benchmarking)
WITH state_totals AS (
    SELECT
        state,
        SUM(transaction_amount) AS state_amount,
        SUM(transaction_count)  AS state_count
    FROM Aggregated_transaction
    GROUP BY state
),
national AS (
    SELECT
        AVG(state_amount) AS nat_avg_amount,
        AVG(state_count)  AS nat_avg_count
    FROM state_totals
)
SELECT
    s.state,
    s.state_amount,
    s.state_count,
    ROUND(s.state_amount / NULLIF(s.state_count, 0), 2)                     AS avg_txn_value,
    ROUND((s.state_amount - n.nat_avg_amount) / NULLIF(n.nat_avg_amount, 0) * 100, 2) AS vs_national_pct,
    CASE
        WHEN s.state_amount > n.nat_avg_amount THEN 'Above Average'
        ELSE 'Below Average'
    END AS performance_flag
FROM state_totals s, national n
ORDER BY vs_national_pct DESC;


-- 1D: Declining / stagnant states (QoQ negative growth for 2+ consecutive quarters)
WITH q_totals AS (
    SELECT state, year, quarter,
           SUM(transaction_amount) AS amount,
           LAG(SUM(transaction_amount)) OVER (
               PARTITION BY state ORDER BY year, quarter
           ) AS prev_amount
    FROM Aggregated_transaction
    GROUP BY state, year, quarter
)
SELECT
    state, year, quarter, amount, prev_amount,
    ROUND((amount - prev_amount) / NULLIF(prev_amount, 0) * 100, 2) AS qoq_pct
FROM q_totals
WHERE amount < prev_amount  -- Negative growth quarters
ORDER BY state, year, quarter;


-- ================================================================
-- BUSINESS CASE 2: Device Dominance & User Engagement Analysis
-- ================================================================
-- Goal: Understand device brand preferences, identify
--       underutilised devices, and measure engagement ratios.

-- 2A: Device brand market share (national)
SELECT
    brand,
    SUM(user_count)                                              AS total_users,
    ROUND(100.0 * SUM(user_count) /
          SUM(SUM(user_count)) OVER (), 2)                      AS market_share_pct
FROM Aggregated_user
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY total_users DESC;


-- 2B: Device brand share per state
SELECT
    state,
    brand,
    SUM(user_count)                                              AS brand_users,
    ROUND(100.0 * SUM(user_count) /
          SUM(SUM(user_count)) OVER (PARTITION BY state), 2)    AS state_share_pct
FROM Aggregated_user
WHERE brand IS NOT NULL
GROUP BY state, brand
ORDER BY state, brand_users DESC;


-- 2C: Engagement ratio per state and quarter
-- (app_opens / registered_users — measures how active users are)
SELECT
    state,
    year,
    quarter,
    MAX(registered_users)                                        AS registered_users,
    MAX(app_opens)                                               AS app_opens,
    ROUND(MAX(app_opens) / NULLIF(MAX(registered_users), 0), 4) AS engagement_ratio
FROM Aggregated_user
GROUP BY state, year, quarter
ORDER BY engagement_ratio DESC;


-- 2D: Underutilised brands (high market share, low engagement signal)
-- States where a brand has ≥15% share but ranks in the bottom half of opens/user
WITH brand_metrics AS (
    SELECT
        state,
        brand,
        SUM(user_count)                                          AS users,
        ROUND(100.0 * SUM(user_count) /
              SUM(SUM(user_count)) OVER (PARTITION BY state), 2) AS state_share_pct
    FROM Aggregated_user
    WHERE brand IS NOT NULL
    GROUP BY state, brand
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY state ORDER BY state_share_pct DESC) AS brand_rank
    FROM brand_metrics
)
SELECT *
FROM ranked
WHERE state_share_pct >= 15      -- significant market share
  AND brand_rank > 3             -- yet not a top-3 brand overall → underutilised
ORDER BY state, state_share_pct DESC;


-- 2E: Year-on-year user growth rate per state
WITH annual AS (
    SELECT state, year,
           MAX(registered_users) AS reg_users
    FROM Aggregated_user
    GROUP BY state, year
)
SELECT
    state,
    year,
    reg_users,
    LAG(reg_users) OVER (PARTITION BY state ORDER BY year) AS prev_year_users,
    ROUND((reg_users - LAG(reg_users) OVER (PARTITION BY state ORDER BY year))
          / NULLIF(LAG(reg_users) OVER (PARTITION BY state ORDER BY year), 0) * 100, 2) AS yoy_growth_pct
FROM annual
ORDER BY state, year;


-- ================================================================
-- BUSINESS CASE 3: Insurance Penetration & Growth Potential
-- ================================================================
-- Goal: Analyze insurance growth and find low-penetration states
--       to prioritize for marketing and insurer partnerships.

-- 3A: Insurance penetration rate per state
SELECT
    ai.state,
    SUM(ai.insurance_count)                                      AS total_policies,
    SUM(ai.insurance_amount)                                     AS total_premium,
    ROUND(SUM(ai.insurance_amount)
          / NULLIF(SUM(ai.insurance_count), 0), 0)              AS avg_premium_per_policy,
    au.total_reg_users,
    ROUND(100.0 * SUM(ai.insurance_count)
          / NULLIF(au.total_reg_users, 0), 4)                   AS penetration_pct
FROM Aggregated_insurance ai
LEFT JOIN (
    SELECT state, SUM(registered_users) AS total_reg_users
    FROM Aggregated_user
    GROUP BY state
) au ON ai.state = au.state
GROUP BY ai.state, au.total_reg_users
ORDER BY penetration_pct ASC;   -- ascending = lowest penetration first (growth targets)


-- 3B: Year-on-year insurance growth trajectory
WITH annual AS (
    SELECT
        year,
        SUM(insurance_count)  AS policies,
        SUM(insurance_amount) AS premium
    FROM Aggregated_insurance
    GROUP BY year
)
SELECT
    year,
    policies,
    premium,
    LAG(policies) OVER (ORDER BY year) AS prev_policies,
    ROUND((policies - LAG(policies) OVER (ORDER BY year))
          / NULLIF(LAG(policies) OVER (ORDER BY year), 0) * 100, 2) AS yoy_policy_growth,
    ROUND((premium - LAG(premium) OVER (ORDER BY year))
          / NULLIF(LAG(premium) OVER (ORDER BY year), 0) * 100, 2)  AS yoy_premium_growth
FROM annual
ORDER BY year;


-- 3C: Opportunity score = large user base but low penetration
WITH pen AS (
    SELECT ai.state,
           SUM(ai.insurance_count)  AS policies,
           au.total_users,
           ROUND(100.0 * SUM(ai.insurance_count)
                 / NULLIF(au.total_users, 0), 4) AS penetration_pct
    FROM Aggregated_insurance ai
    LEFT JOIN (
        SELECT state, SUM(registered_users) AS total_users
        FROM Aggregated_user GROUP BY state
    ) au ON ai.state = au.state
    GROUP BY ai.state, au.total_users
),
scored AS (
    SELECT *,
           ROUND(
               (total_users / MAX(total_users) OVER ()) -
               (penetration_pct / MAX(penetration_pct) OVER ()),
           3) AS opportunity_score
    FROM pen
)
SELECT *,
    CASE
        WHEN opportunity_score >= 0.3 THEN 'Critical Priority'
        WHEN opportunity_score >= 0.1 THEN 'High Priority'
        WHEN opportunity_score >= 0.0 THEN 'Medium Priority'
        ELSE 'Low Priority'
    END AS campaign_priority
FROM scored
ORDER BY opportunity_score DESC;


-- 3D: Quarterly insurance trend to spot acceleration
SELECT
    year,
    quarter,
    SUM(insurance_count)  AS policies,
    SUM(insurance_amount) AS premium,
    LAG(SUM(insurance_count)) OVER (ORDER BY year, quarter) AS prev_policies,
    ROUND(
        (SUM(insurance_count) - LAG(SUM(insurance_count)) OVER (ORDER BY year, quarter))
        / NULLIF(LAG(SUM(insurance_count)) OVER (ORDER BY year, quarter), 0) * 100,
    2) AS qoq_growth_pct
FROM Aggregated_insurance
GROUP BY year, quarter
ORDER BY year, quarter;


-- ================================================================
-- BUSINESS CASE 7: Transaction Analysis Across States & Districts
-- ================================================================
-- Goal: Identify top-performing states, districts, and pin codes
--       by transaction volume and value.

-- 7A: Top 10 states by transaction value
SELECT
    state,
    SUM(transaction_count)  AS total_count,
    SUM(transaction_amount) AS total_amount,
    ROUND(SUM(transaction_amount) / NULLIF(SUM(transaction_count), 0), 0) AS avg_txn_value
FROM Top_transaction
GROUP BY state
ORDER BY total_amount DESC
LIMIT 10;


-- 7B: Top 10 districts overall
SELECT
    state,
    district,
    SUM(transaction_count)  AS total_count,
    SUM(transaction_amount) AS total_amount
FROM Map_transaction
WHERE district IS NOT NULL
GROUP BY state, district
ORDER BY total_amount DESC
LIMIT 10;


-- 7C: Top districts within a specific state (parameterisable)
SELECT
    state,
    district,
    SUM(transaction_count)  AS total_count,
    SUM(transaction_amount) AS total_amount,
    ROUND(SUM(transaction_amount) / NULLIF(SUM(transaction_count), 0), 0) AS avg_txn_value
FROM Map_transaction
WHERE state = 'Maharashtra'          -- << replace with target state
  AND district IS NOT NULL
GROUP BY state, district
ORDER BY total_amount DESC
LIMIT 10;


-- 7D: Top 10 pin codes by transaction value
SELECT
    state,
    pincode,
    SUM(transaction_count)  AS total_count,
    SUM(transaction_amount) AS total_amount
FROM Top_transaction
WHERE pincode IS NOT NULL
GROUP BY state, pincode
ORDER BY total_amount DESC
LIMIT 10;


-- 7E: Quarterly trend per state for time-series comparison
SELECT
    state,
    year,
    quarter,
    SUM(transaction_count)  AS total_count,
    SUM(transaction_amount) AS total_amount,
    LAG(SUM(transaction_amount)) OVER (
        PARTITION BY state ORDER BY year, quarter
    ) AS prev_amount,
    ROUND(
        (SUM(transaction_amount) - LAG(SUM(transaction_amount)) OVER (
            PARTITION BY state ORDER BY year, quarter))
        / NULLIF(LAG(SUM(transaction_amount)) OVER (
            PARTITION BY state ORDER BY year, quarter), 0) * 100,
    2) AS qoq_growth_pct
FROM Map_transaction
GROUP BY state, year, quarter
ORDER BY state, year, quarter;


-- 7F: District-level concentration index (Herfindahl)
-- Measures whether a state's value is concentrated in one district or spread
WITH dist_amounts AS (
    SELECT state, district,
           SUM(transaction_amount) AS dist_amount
    FROM Map_transaction
    GROUP BY state, district
),
state_totals AS (
    SELECT state, SUM(transaction_amount) AS state_amount
    FROM Map_transaction
    GROUP BY state
),
shares AS (
    SELECT d.state, d.district,
           ROUND(d.dist_amount / NULLIF(s.state_amount, 0), 4) AS share
    FROM dist_amounts d
    JOIN state_totals s ON d.state = s.state
)
SELECT state,
       ROUND(SUM(share * share), 4)  AS hhi_concentration_index,
       CASE
           WHEN SUM(share * share) > 0.25 THEN 'Highly Concentrated'
           WHEN SUM(share * share) > 0.10 THEN 'Moderately Concentrated'
           ELSE 'Well Distributed'
       END AS distribution_type
FROM shares
GROUP BY state
ORDER BY hhi_concentration_index DESC;


-- ================================================================
-- BUSINESS CASE 8: User Registration Analysis
-- ================================================================
-- Goal: Identify top states, districts, and pin codes by
--       registrations in a specific year-quarter combination.

-- 8A: Top 10 states in a given year-quarter
SELECT
    state,
    SUM(registered_users) AS total_registered
FROM Top_user
WHERE year = 2024 AND quarter = 4   -- << replace year/quarter
GROUP BY state
ORDER BY total_registered DESC
LIMIT 10;


-- 8B: Top 10 districts in a given year-quarter
SELECT
    state,
    district,
    SUM(registered_users) AS total_registered
FROM Top_user
WHERE year = 2024 AND quarter = 4   -- << replace year/quarter
  AND district IS NOT NULL
GROUP BY state, district
ORDER BY total_registered DESC
LIMIT 10;


-- 8C: Top 10 pin codes in a given year-quarter
SELECT
    state,
    pincode,
    SUM(registered_users) AS total_registered
FROM Top_user
WHERE year = 2024 AND quarter = 4   -- << replace year/quarter
  AND pincode IS NOT NULL
GROUP BY state, pincode
ORDER BY total_registered DESC
LIMIT 10;


-- 8D: Quarter-on-quarter registration growth per state
WITH qtr_regs AS (
    SELECT state, year, quarter,
           SUM(registered_users) AS regs
    FROM Top_user
    GROUP BY state, year, quarter
),
with_lag AS (
    SELECT *,
        LAG(regs) OVER (PARTITION BY state ORDER BY year, quarter) AS prev_regs
    FROM qtr_regs
)
SELECT
    state, year, quarter, regs, prev_regs,
    ROUND((regs - prev_regs) / NULLIF(prev_regs, 0) * 100, 2) AS qoq_growth_pct
FROM with_lag
ORDER BY qoq_growth_pct DESC
LIMIT 20;


-- 8E: Growth hotspot detection — fastest acceleration in last 4 quarters
WITH recent AS (
    SELECT state, year, quarter,
           SUM(registered_users) AS regs
    FROM Top_user
    WHERE (year = 2024)
       OR (year = 2023 AND quarter >= 3)
    GROUP BY state, year, quarter
),
ranked AS (
    SELECT *,
           FIRST_VALUE(regs) OVER (PARTITION BY state ORDER BY year, quarter ASC)  AS first_regs,
           LAST_VALUE(regs)  OVER (PARTITION BY state ORDER BY year, quarter
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_regs
    FROM recent
)
SELECT DISTINCT
    state,
    first_regs  AS regs_start,
    last_regs   AS regs_end,
    ROUND((last_regs - first_regs) / NULLIF(first_regs, 0) * 100, 2) AS growth_over_period_pct
FROM ranked
ORDER BY growth_over_period_pct DESC
LIMIT 10;


-- 8F: States with declining registrations (early churn signal)
WITH qtr_regs AS (
    SELECT state, year, quarter,
           SUM(registered_users) AS regs,
           LAG(SUM(registered_users)) OVER (
               PARTITION BY state ORDER BY year, quarter
           ) AS prev_regs
    FROM Top_user
    GROUP BY state, year, quarter
)
SELECT state, year, quarter, regs, prev_regs,
       ROUND((regs - prev_regs) / NULLIF(prev_regs, 0) * 100, 2) AS qoq_growth_pct
FROM qtr_regs
WHERE regs < prev_regs
ORDER BY qoq_growth_pct ASC;
