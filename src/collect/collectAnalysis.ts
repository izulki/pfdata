

const pgp = require('pg-promise')(); //User Postgres Helpers

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface collectAnalysisResponse {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectAnalysis(db: any, method: string): Promise<collectAnalysisResponse> {
    // --- CALCULATE CHANGE PAST 3 DAYS ---
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectAnalysis", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectAnalysis' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/collectAnalysis/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")
    //Get cards and its prices from today and three days ago.
    logger.info("Fetch Cards and Prices")
    let data = await db.any(QUERY_pfanalysis_price_changes_daily, [true]);
    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectAnalysis/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: collectAnalysisResponse = {
        state: state,
        errors: errors,
        log: logid
    }
    
    if ( errors == 0) {
        result.state = true;
        return Promise.resolve(result)
    } else {
        result.state = false;
        return Promise.reject(result)
    }
}

let QUERY_pfanalysis_price_changes_daily = `
-- First create temp table
DROP TABLE IF EXISTS temp_price_changes;
WITH latest_date AS (
  SELECT updatedsource::date as date
  FROM pf_cards_price_history
  WHERE updatedsource IS NOT NULL
  ORDER BY updatedsource DESC
  LIMIT 1
),
current_prices AS (
  SELECT t.cardid, t.variant, t.price, t.updatedsource
  FROM pf_cards_price_history t, latest_date ld
  WHERE t.updatedsource::date = ld.date
),
interval_prices AS (
  SELECT DISTINCT ON (t.cardid, t.variant, interval_type)
    t.cardid,
    t.variant,
    ph.price as interval_price,
    ph.updatedsource::date as interval_date,
    interval_type
  FROM current_prices t
  CROSS JOIN (VALUES 
    ('1w', INTERVAL '7 days'),
    ('1m', INTERVAL '1 month'),
    ('3m', INTERVAL '3 months'),
    ('6m', INTERVAL '6 months'),
    ('1y', INTERVAL '1 year')
  ) as intervals(interval_type, interval_length)
  LEFT JOIN pf_cards_price_history ph
    ON t.cardid = ph.cardid
    AND t.variant = ph.variant
    AND ph.updatedsource::date <= t.updatedsource::date - interval_length
    AND ph.updatedsource::date > t.updatedsource::date - (interval_length + INTERVAL '7 days')
  ORDER BY t.cardid, t.variant, interval_type, ph.updatedsource::date DESC
)
SELECT 
  -- Existing columns
  t.cardid,
  t.variant,
  t.price as current_price,
  p1d.price as previous_price,
  ((t.price - p1d.price) / NULLIF(p1d.price, 0)) * 100 as percentage_change,
  t.updatedsource::date as latest_update_date,
  t.updatedsource::date - INTERVAL '3 days' as previous_update_date,
  -- New interval columns
  -- Weekly changes
  p1w.interval_price as previous_price_1w,
  (t.price - p1w.interval_price) as price_change_1w,
  ((t.price - p1w.interval_price) / NULLIF(p1w.interval_price, 0)) * 100 as percentage_change_1w,
  -- Monthly changes
  p1m.interval_price as previous_price_1m,
  (t.price - p1m.interval_price) as price_change_1m,
  ((t.price - p1m.interval_price) / NULLIF(p1m.interval_price, 0)) * 100 as percentage_change_1m,
  -- 3 Month changes
  p3m.interval_price as previous_price_3m,
  (t.price - p3m.interval_price) as price_change_3m,
  ((t.price - p3m.interval_price) / NULLIF(p3m.interval_price, 0)) * 100 as percentage_change_3m,
  -- 6 Month changes
  p6m.interval_price as previous_price_6m,
  (t.price - p6m.interval_price) as price_change_6m,
  ((t.price - p6m.interval_price) / NULLIF(p6m.interval_price, 0)) * 100 as percentage_change_6m,
  -- Yearly changes
  p1y.interval_price as previous_price_1y,
  (t.price - p1y.interval_price) as price_change_1y,
  ((t.price - p1y.interval_price) / NULLIF(p1y.interval_price, 0)) * 100 as percentage_change_1y
INTO TEMPORARY TABLE temp_price_changes
FROM current_prices t
LEFT JOIN pf_cards_price_history p1d  -- 1 day
  ON t.cardid = p1d.cardid 
  AND t.variant = p1d.variant
  AND p1d.updatedsource::date = t.updatedsource::date - INTERVAL '1 day'
LEFT JOIN interval_prices p1w
  ON t.cardid = p1w.cardid 
  AND t.variant = p1w.variant 
  AND p1w.interval_type = '1w'
LEFT JOIN interval_prices p1m
  ON t.cardid = p1m.cardid 
  AND t.variant = p1m.variant 
  AND p1m.interval_type = '1m'
LEFT JOIN interval_prices p3m
  ON t.cardid = p3m.cardid 
  AND t.variant = p3m.variant 
  AND p3m.interval_type = '3m'
LEFT JOIN interval_prices p6m
  ON t.cardid = p6m.cardid 
  AND t.variant = p6m.variant 
  AND p6m.interval_type = '6m'
LEFT JOIN interval_prices p1y
  ON t.cardid = p1y.cardid 
  AND t.variant = p1y.variant 
  AND p1y.interval_type = '1y';

-- Then do the update in a transaction if we have data
BEGIN;
TRUNCATE TABLE pfanalysis_price_changes_daily;
    
INSERT INTO pfanalysis_price_changes_daily 
SELECT * FROM temp_price_changes;
COMMIT;
`