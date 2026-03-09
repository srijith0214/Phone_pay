-- ============================================================
-- PhonePe Transaction Insights - SQL Schema & Business Queries
-- ============================================================

-- ────────────────────────────────────────────────────────────────
-- SCHEMA CREATION
-- ────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS phonepe_pulse;
USE phonepe_pulse;

-- Aggregated Tables
CREATE TABLE IF NOT EXISTS Aggregated_transaction (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100) NOT NULL,
    year                SMALLINT NOT NULL,
    quarter             TINYINT NOT NULL,
    transaction_type    VARCHAR(100) NOT NULL,
    transaction_count   BIGINT NOT NULL,
    transaction_amount  DECIMAL(20,2) NOT NULL,
    INDEX idx_state_year (state, year, quarter),
    INDEX idx_type (transaction_type)
);

CREATE TABLE IF NOT EXISTS Aggregated_user (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100) NOT NULL,
    year                SMALLINT NOT NULL,
    quarter             TINYINT NOT NULL,
    brand               VARCHAR(100),
    user_count          BIGINT,
    registered_users    BIGINT,
    app_opens           BIGINT,
    INDEX idx_state_year (state, year, quarter),
    INDEX idx_brand (brand)
);

CREATE TABLE IF NOT EXISTS Aggregated_insurance (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100) NOT NULL,
    year                SMALLINT NOT NULL,
    quarter             TINYINT NOT NULL,
    insurance_count     BIGINT,
    insurance_amount    DECIMAL(20,2),
    INDEX idx_state_year (state, year, quarter)
);

-- Map Tables
CREATE TABLE IF NOT EXISTS Map_transaction (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    year                SMALLINT,
    quarter             TINYINT,
    transaction_count   BIGINT,
    transaction_amount  DECIMAL(20,2),
    INDEX idx_state_district (state, district)
);

CREATE TABLE IF NOT EXISTS Map_user (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    year                SMALLINT,
    quarter             TINYINT,
    registered_users    BIGINT,
    app_opens           BIGINT,
    INDEX idx_state_district (state, district)
);

CREATE TABLE IF NOT EXISTS Map_insurance (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    year                SMALLINT,
    quarter             TINYINT,
    insurance_count     BIGINT,
    insurance_amount    DECIMAL(20,2),
    INDEX idx_state_district (state, district)
);

-- Top Tables
CREATE TABLE IF NOT EXISTS Top_transaction (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    pincode             VARCHAR(20),
    year                SMALLINT,
    quarter             TINYINT,
    transaction_count   BIGINT,
    transaction_amount  DECIMAL(20,2)
);

CREATE TABLE IF NOT EXISTS Top_user (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    pincode             VARCHAR(20),
    year                SMALLINT,
    quarter             TINYINT,
    registered_users    BIGINT
);

CREATE TABLE IF NOT EXISTS Top_insurance (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100),
    district            VARCHAR(200),
    pincode             VARCHAR(20),
    year                SMALLINT,
    quarter             TINYINT,
    insurance_count     BIGINT,
    insurance_amount    DECIMAL(20,2)
);

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 1: CUSTOMER SEGMENTATION
-- Identify distinct user groups based on spending habits
-- ────────────────────────────────────────────────────────────────

-- State-level spending segments
SELECT
    state,
    SUM(transaction_amount)                         AS total_amount,
    SUM(transaction_count)                          AS total_count,
    ROUND(SUM(transaction_amount)/SUM(transaction_count), 2) AS avg_txn_value,
    CASE
        WHEN SUM(transaction_amount) >= (SELECT PERCENTILE_CONT(0.66)
            WITHIN GROUP (ORDER BY total_amt) FROM
            (SELECT state, SUM(transaction_amount) AS total_amt
             FROM Aggregated_transaction GROUP BY state) t)
         AND SUM(transaction_count)  >= (SELECT PERCENTILE_CONT(0.66)
            WITHIN GROUP (ORDER BY total_cnt) FROM
            (SELECT state, SUM(transaction_count) AS total_cnt
             FROM Aggregated_transaction GROUP BY state) t)
            THEN 'High Value & High Volume'
        WHEN SUM(transaction_amount) >= (SELECT AVG(total_amt) FROM
            (SELECT SUM(transaction_amount) AS total_amt
             FROM Aggregated_transaction GROUP BY state) t)
            THEN 'High Value'
        WHEN SUM(transaction_count)  >= (SELECT AVG(total_cnt) FROM
            (SELECT SUM(transaction_count) AS total_cnt
             FROM Aggregated_transaction GROUP BY state) t)
            THEN 'High Volume'
        ELSE 'Growth Potential'
    END                                             AS segment
FROM Aggregated_transaction
GROUP BY state
ORDER BY total_amount DESC;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 2: FRAUD DETECTION SIGNALS
-- States with anomalous average transaction values (z-score > 2)
-- ────────────────────────────────────────────────────────────────

WITH state_stats AS (
    SELECT
        state,
        SUM(transaction_count)   AS total_count,
        SUM(transaction_amount)  AS total_amount,
        SUM(transaction_amount) / NULLIF(SUM(transaction_count), 0) AS avg_txn
    FROM Aggregated_transaction
    GROUP BY state
),
global_stats AS (
    SELECT
        AVG(avg_txn) AS mean_avg,
        STDDEV(avg_txn) AS std_avg
    FROM state_stats
)
SELECT
    s.state,
    s.avg_txn,
    s.total_count,
    s.total_amount,
    ROUND((s.avg_txn - g.mean_avg) / NULLIF(g.std_avg, 0), 2) AS z_score,
    CASE WHEN ABS((s.avg_txn - g.mean_avg)/NULLIF(g.std_avg,0)) > 2
         THEN 'ANOMALY' ELSE 'NORMAL' END AS fraud_signal
FROM state_stats s, global_stats g
ORDER BY ABS((s.avg_txn - g.mean_avg) / NULLIF(g.std_avg, 0)) DESC;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 3: GEOGRAPHICAL INSIGHTS
-- State and district level totals for mapping
-- ────────────────────────────────────────────────────────────────

-- State-level map data
SELECT
    state,
    SUM(transaction_amount) AS total_amount,
    SUM(transaction_count)  AS total_count
FROM Map_transaction
GROUP BY state
ORDER BY total_amount DESC;

-- District-level map data
SELECT
    state,
    district,
    SUM(transaction_amount) AS total_amount,
    SUM(transaction_count)  AS total_count
FROM Map_transaction
GROUP BY state, district
ORDER BY total_amount DESC
LIMIT 50;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 4: PAYMENT PERFORMANCE
-- Category-wise performance for strategic investments
-- ────────────────────────────────────────────────────────────────

SELECT
    transaction_type,
    SUM(transaction_count)                               AS total_count,
    SUM(transaction_amount)                              AS total_amount,
    ROUND(SUM(transaction_amount)/SUM(transaction_count),2) AS avg_txn_value,
    ROUND(100.0 * SUM(transaction_count) /
        (SELECT SUM(transaction_count) FROM Aggregated_transaction), 2)
                                                         AS count_share_pct,
    ROUND(100.0 * SUM(transaction_amount) /
        (SELECT SUM(transaction_amount) FROM Aggregated_transaction), 2)
                                                         AS amount_share_pct
FROM Aggregated_transaction
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 5: USER ENGAGEMENT
-- Quarterly active user trends and app-open rates
-- ────────────────────────────────────────────────────────────────

SELECT
    year,
    quarter,
    SUM(registered_users)           AS total_registered,
    SUM(app_opens)                  AS total_app_opens,
    ROUND(SUM(app_opens) /
          NULLIF(SUM(registered_users), 0), 2) AS engagement_ratio
FROM Aggregated_user
GROUP BY year, quarter
ORDER BY year, quarter;

-- Device brand market share
SELECT
    brand,
    SUM(user_count)                                              AS total_users,
    ROUND(100.0 * SUM(user_count) /
          (SELECT SUM(user_count) FROM Aggregated_user WHERE brand IS NOT NULL), 2)
                                                                  AS market_share_pct
FROM Aggregated_user
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY total_users DESC;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 6: INSURANCE INSIGHTS
-- Insurance product performance and user experience data
-- ────────────────────────────────────────────────────────────────

-- State-wise insurance penetration
SELECT
    ai.state,
    SUM(ai.insurance_count)                          AS total_policies,
    SUM(ai.insurance_amount)                         AS total_premium,
    ROUND(SUM(ai.insurance_amount) /
          NULLIF(SUM(ai.insurance_count), 0), 2)     AS avg_premium,
    ROUND(100.0 * SUM(ai.insurance_count) /
          NULLIF(au.total_users, 0), 4)              AS penetration_rate
FROM Aggregated_insurance ai
LEFT JOIN (
    SELECT state, SUM(registered_users) AS total_users
    FROM Aggregated_user GROUP BY state
) au ON ai.state = au.state
GROUP BY ai.state, au.total_users
ORDER BY total_premium DESC;

-- Quarterly insurance trend
SELECT
    year,
    quarter,
    SUM(insurance_count)  AS total_policies,
    SUM(insurance_amount) AS total_premium,
    ROUND(SUM(insurance_amount) /
          NULLIF(SUM(insurance_count), 0), 2) AS avg_premium
FROM Aggregated_insurance
GROUP BY year, quarter
ORDER BY year, quarter;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 7: MARKETING OPTIMIZATION
-- User behavior patterns to target campaigns
-- ────────────────────────────────────────────────────────────────

SELECT
    at.state,
    at.transaction_type,
    SUM(at.transaction_count)   AS txn_count,
    SUM(at.transaction_amount)  AS txn_amount,
    au.total_users,
    ROUND(SUM(at.transaction_amount) /
          NULLIF(au.total_users, 0), 2) AS revenue_per_user
FROM Aggregated_transaction at
LEFT JOIN (
    SELECT state, SUM(registered_users) AS total_users
    FROM Aggregated_user GROUP BY state
) au ON at.state = au.state
GROUP BY at.state, at.transaction_type, au.total_users
ORDER BY revenue_per_user DESC;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 8: TREND ANALYSIS
-- Quarter-on-quarter and year-on-year growth
-- ────────────────────────────────────────────────────────────────

WITH quarterly AS (
    SELECT
        year,
        quarter,
        SUM(transaction_amount) AS total_amount,
        SUM(transaction_count)  AS total_count,
        LAG(SUM(transaction_amount)) OVER (ORDER BY year, quarter) AS prev_amount
    FROM Aggregated_transaction
    GROUP BY year, quarter
)
SELECT
    year,
    quarter,
    total_amount,
    total_count,
    prev_amount,
    ROUND((total_amount - prev_amount) /
          NULLIF(prev_amount, 0) * 100, 2) AS qoq_growth_pct
FROM quarterly
ORDER BY year, quarter;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 9: TOP PERFORMERS
-- Top states, districts, and pincodes
-- ────────────────────────────────────────────────────────────────

-- Top 10 states
SELECT state,
       SUM(transaction_count)  AS total_count,
       SUM(transaction_amount) AS total_amount
FROM Top_transaction
GROUP BY state
ORDER BY total_amount DESC
LIMIT 10;

-- Top 10 districts
SELECT state, district,
       SUM(transaction_count)  AS total_count,
       SUM(transaction_amount) AS total_amount
FROM Top_transaction
WHERE district IS NOT NULL
GROUP BY state, district
ORDER BY total_amount DESC
LIMIT 10;

-- Top 10 pincodes
SELECT state, pincode,
       SUM(transaction_count)  AS total_count,
       SUM(transaction_amount) AS total_amount
FROM Top_transaction
WHERE pincode IS NOT NULL
GROUP BY state, pincode
ORDER BY total_amount DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────────
-- BUSINESS CASE 10: COMPETITIVE BENCHMARKING
-- State performance vs national average
-- ────────────────────────────────────────────────────────────────

WITH national AS (
    SELECT
        AVG(state_amount) AS nat_avg_amount,
        AVG(state_count)  AS nat_avg_count
    FROM (
        SELECT state,
               SUM(transaction_amount) AS state_amount,
               SUM(transaction_count)  AS state_count
        FROM Aggregated_transaction
        GROUP BY state
    ) s
)
SELECT
    a.state,
    SUM(a.transaction_amount) AS state_amount,
    SUM(a.transaction_count)  AS state_count,
    n.nat_avg_amount,
    ROUND((SUM(a.transaction_amount) - n.nat_avg_amount)
          / NULLIF(n.nat_avg_amount, 0) * 100, 2) AS vs_national_pct
FROM Aggregated_transaction a, national n
GROUP BY a.state, n.nat_avg_amount, n.nat_avg_count
ORDER BY vs_national_pct DESC;
