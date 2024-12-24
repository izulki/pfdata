

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
)
SELECT 
  t.cardid,
  t.variant,
  t.price as current_price,
  p.price as previous_price,
  ((t.price - p.price) / p.price) * 100 as percentage_change,
  ld.date as latest_update_date,
  ld.date - INTERVAL '3 days' as previous_update_date
INTO TEMPORARY TABLE temp_price_changes
FROM latest_date ld,
     pf_cards_price_history t  
JOIN pf_cards_price_history p  
  USING (cardid, variant)
WHERE t.updatedsource::date = ld.date
  AND p.updatedsource::date = ld.date - INTERVAL '3 days'
  AND t.price != p.price;

-- Then do the update in a transaction if we have data
BEGIN;
TRUNCATE TABLE pfanalysis_price_changes_daily;
    
INSERT INTO pfanalysis_price_changes_daily 
SELECT * FROM temp_price_changes;
COMMIT;
`