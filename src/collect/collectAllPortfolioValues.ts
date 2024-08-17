const pgp = require('pg-promise')(); //User Postgres Helpers
const dayjs = require('dayjs');

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface CollectAllPortfolioValues {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectAllPortfolioValues(db: any, method: string): Promise<CollectAllPortfolioValues> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectAllPortfolioValues", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'AllPortfolioValues' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/AllPortfolioValues/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")

    let query = `
        insert INTO user_metrics_log (userid, total_cards, total_unique_cards, unique_cards_with_variant, total_value, log_date)
        SELECT 
            uc.userid,
            COUNT(*) AS total_cards,
            COUNT(DISTINCT uc.cardid) AS total_unique_cards,
            COUNT(DISTINCT CONCAT(uc.cardid, '-', uc.variant)) AS unique_cards_with_variant,
            SUM(
                (SELECT price 
                FROM pfdata_quickprice 
                WHERE cardid = uc.cardid 
                AND variant = uc.variant 
                order BY updated DESC 
                LIMIT 1)
            ) AS total_value,
            CURRENT_DATE AS log_date  -- Use CURRENT_DATE to set the log_date
            from pf_users_card_inventory uc
        group BY 
            uc.userid
        ON CONFLICT (userid, log_date) DO NOTHING;
    `
    await db.any(query, [true]);

    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/AllPortfolioValues/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: CollectAllPortfolioValues = {
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