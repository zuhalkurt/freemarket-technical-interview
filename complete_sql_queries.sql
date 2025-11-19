-- This document contains all SQL queries related to interview assessment.
-- SQL Dialect: Snowflake


-- List of completed internal transfers between different pods in Q1 2025
-- This query retrieves all internal transfers that were completed between January 1, 2025,
-- and March 31, 2025, where the sender and receiver belong to different pods

SELECT
    it.sender_account_id,
    sa.account_name        AS SenderAccountName,
    sg.group_id            AS SenderGroupId,
    sg.group_name          AS SenderGroupName,
    sg.group_pod           AS SenderGroupPod,
    it.receiver_account_id,
    ra.account_name        AS ReceiverAccountName,
    rg.group_id            AS ReceiverGroupId,
    rg.group_name          AS ReceiverGroupName,
    rg.group_pod           AS ReceiverGroupPod,
     it.amt,
    it.currency,
    it.transfer_status,
    it.transfer_time
FROM internal_transfers it
JOIN account sa
    ON it.sender_account_id = sa.account_id
JOIN client sc
    ON sa.client_id = sc.client_id
JOIN "GROUP" sg
    ON sc.group_id = sg.group_id
JOIN account ra
    ON it.receiver_account_id = ra.account_id
JOIN client rc
    ON ra.client_id = rc.client_id
JOIN "GROUP" rg
    ON rc.group_id = rg.group_id
WHERE it.transfer_status = 'completed'
  AND it.transfer_time >= '2025-01-01'
  AND it.transfer_time <  '2025-04-01'
  AND sg.group_pod <> rg.group_pod;


-- 2. Total GBP-Normalised Transfers for Vertical = 'Gambling', Live Clients
-- Assumptions:
--  - We count transfers where the SENDER's client is in the 'Gambling' vertical
--  - A client is "live" if they have at least one live account (AccountStatus = 'live')
--  - Normalisation uses DailyExchangeRate where FromCurrency = transfer currency
--    and ToCurrency = 'GBP' on the transfer date

WITH live_gambling_clients AS (
    SELECT
        c.client_id,
        c.client_name
    FROM CLIENT c
    JOIN account a
        ON a.client_id = c.client_id
    WHERE c.vertical = 'Gambling'
    GROUP BY c.client_id, c.client_name
    HAVING MAX(CASE WHEN a.account_status = 'live' THEN 1 ELSE 0 END) = 1
),

completed_transfers_2024 AS (
    SELECT
        it.sender_account_id,
        it.receiver_account_id,
        it.amt,
        it.currency,
        it.transfer_status,
        it.transfer_time,
        TO_DATE(it.transfer_time) AS transfer_date
    FROM internal_transfers it
    JOIN ACCOUNT sa
        ON it.sender_account_id = sa.account_id
    JOIN live_gambling_clients lgc
        ON sa.client_id = lgc.client_id
    WHERE it.transfer_status = 'completed'
      AND it.transfer_time >= '2024-01-01'
      AND it.transfer_time <  '2025-01-01'
),

transfers_with_gbp AS (
    SELECT
        ct.sender_account_id,
        sa.client_id          AS sender_client_id,
        ct.amt,
        ct.Currency,
        der.rate,
        ct.amt * der.rate    AS amt_gbp
    FROM completed_transfers_2024 ct
    JOIN ACCOUNT sa
        ON ct.sender_account_id = sa.account_id
    JOIN daily_exchange_rate der
        ON der.from_currency = ct.currency
       AND der.to_currency   = 'GBP'
       AND der.date         = ct.transfer_date
)
SELECT
    c.client_id,
    c.client_name,
    SUM(tg.amt_gbp) AS total_gbp_normalised_amt_2024
FROM transfers_with_gbp tg
JOIN CLIENT c
    ON tg.sender_client_id = c.client_id
GROUP BY
    c.client_id,
    c.client_name
ORDER BY
    total_gbp_normalised_amt_2024 DESC;


-- 3. 7-Day Moving Average by Sender Vertical
--  - Completed transfers only
--  - Amounts normalised to GBP
--  - Last 6 months from the beginning of Q1 2025 and end of Q1 2025 (as sample data only goes to Feb 2025)
--  - Window: 7-day moving average per Sender Vertical

WITH base_transfers AS (
    SELECT
        it.sender_account_id,
        it.amt,
        it.currency,
        it.transfer_status,
        DATE_TRUNC('day', it.transfer_time) AS transfer_date
    FROM internal_transfers it
    WHERE transfer_status = 'completed'
      AND DATE_TRUNC('day', it.transfer_time) >= DATE('2025-01-01')
      AND DATE_TRUNC('day', it.transfer_time) < DATE('2025-04-01')
),

transfers_with_sender_vertical AS (
    SELECT
        bt.transfer_date,
        c.vertical AS sender_vertical,
        bt.amt,
        bt.currency
    FROM base_transfers bt
    JOIN account a
        ON bt.sender_account_id = a.account_id
    JOIN client c
        ON a.client_id = c.client_id
),

gbp_normalised AS (
    SELECT
        tsv.transfer_date,
        tsv.sender_vertical,
        tsv.amt,
        tsv.currency,
        der.rate,
        tsv.amt * der.rate AS amt_gbp
    FROM transfers_with_sender_vertical tsv
    JOIN daily_exchange_rate der
        ON der.from_currency = tsv.currency
       AND der.to_currency   = 'GBP'
       AND der.date         = tsv.transfer_date
),

daily_totals AS (
    SELECT
        transfer_date,
        sender_vertical,
        SUM(amt_gbp) AS daily_total_gbp
    FROM gbp_normalised
    GROUP BY
        transfer_date,
        sender_vertical
)

SELECT
    transfer_date,
    sender_vertical,
    daily_total_gbp,
    AVG(daily_total_gbp) OVER (
        PARTITION BY sender_vertical
        ORDER BY transfer_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_weekly_gbp
FROM daily_totals
ORDER BY
    sender_vertical,
    transfer_date;
