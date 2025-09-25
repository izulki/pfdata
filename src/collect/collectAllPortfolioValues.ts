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
            INSERT INTO user_metrics_log (
                userid, 
                total_cards, 
                total_unique_cards, 
                unique_cards_with_variant, 
                total_value, 
                graded,
                sold,
                purchases,
                other_costs,
                sales,
                profit,
                realized_profit,
                unrealized_profit,
                log_date
            )
            WITH card_metrics AS (
                -- Calculate card inventory metrics without transactions to avoid duplication
                SELECT 
                    uc.userid,
                    -- Only count non-sold cards for main inventory metrics
                    COUNT(CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN 1 
                    END) AS total_cards,
                    COUNT(DISTINCT CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN uc.cardid 
                    END) AS total_unique_cards,
                    COUNT(DISTINCT CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN CONCAT(uc.cardid, '-', uc.variant) 
                    END) AS unique_cards_with_variant,
                    -- Use JOINs instead of subqueries to get accurate pricing
                    SUM(CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN COALESCE(graded_prices.current_price, raw_prices.current_price, 0)
                    END) AS total_value,
                    -- Count graded cards that are currently owned (not sold)
                    COUNT(CASE 
                        WHEN (pucid.status < 100 OR pucid.status IS NULL)
                        AND pucid.grade IS NOT NULL 
                        AND pucid.grade != 'Ungraded' 
                        THEN 1 
                    END) AS graded,
                    -- Count sold cards (status >= 100)
                    COUNT(CASE 
                        WHEN pucid.status >= 100 
                        THEN 1 
                    END) AS sold,
                    -- Calculate unrealized profit: current value of owned cards minus their costs
                    SUM(CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN COALESCE(graded_prices.current_price, raw_prices.current_price, 0)
                    END) - SUM(CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN COALESCE(pucid.cost_usd, 0)
                    END) AS unrealized_profit_from_cards
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_card_inventory_details pucid 
                    ON pucid.inventory_id = uc.id 
                -- Join graded prices when card has a grade (not Ungraded or NULL)
                LEFT JOIN pfanalysis_graded_price_changes_daily graded_prices 
                    ON uc.cardid = graded_prices.cardid 
                    AND uc.variant = graded_prices.variant 
                    AND pucid.grade = graded_prices.grade
                    AND pucid.grade IS NOT NULL 
                    AND pucid.grade != 'Ungraded'
                -- Join raw prices for ungraded cards OR as fallback when no graded price found
                LEFT JOIN pfanalysis_price_changes_daily raw_prices 
                    ON uc.cardid = raw_prices.cardid 
                    AND uc.variant = raw_prices.variant
                    AND (pucid.grade IS NULL OR pucid.grade = 'Ungraded' OR graded_prices.current_price IS NULL)
                GROUP BY uc.userid
            ),
            transaction_metrics AS (
                -- Calculate transaction-based metrics separately
                SELECT 
                    uc.userid,
                    -- Sum purchase amounts (transaction_type = 100)
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type = 100 
                        THEN uit.amount_usd 
                    END), 0) AS purchases,
                    -- Sum other costs (shipping, fees, grading, upcharge: 101-104)
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type IN (101, 102, 103, 104) 
                        THEN uit.amount_usd 
                    END), 0) AS other_costs,
                    -- Sum sales amounts (sale, refund: 200, 201)
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type IN (200, 201) 
                        THEN uit.amount_usd 
                    END), 0) AS sales
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_inventory_transactions uit 
                    ON uit.inventory_id = uc.id
                GROUP BY uc.userid
            ),
            realized_profit_metrics AS (
                -- Calculate realized profit from sold cards only
                SELECT 
                    uc.userid,
                    -- Realized profit: profit from sold cards only
                    COALESCE(SUM(CASE 
                        WHEN pucid.status >= 100 AND uit.transaction_type IN (200, 201) 
                        THEN uit.amount_usd 
                    END), 0) - 
                    COALESCE(SUM(CASE 
                        WHEN pucid.status >= 100 AND uit.transaction_type = 100 
                        THEN uit.amount_usd 
                    END), 0) - 
                    COALESCE(SUM(CASE 
                        WHEN pucid.status >= 100 AND uit.transaction_type IN (101, 102, 103, 104) 
                        THEN uit.amount_usd 
                    END), 0) AS realized_profit
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_card_inventory_details pucid 
                    ON pucid.inventory_id = uc.id 
                LEFT JOIN pf_users_inventory_transactions uit 
                    ON uit.inventory_id = uc.id
                GROUP BY uc.userid
            )
            SELECT 
                COALESCE(cm.userid, tm.userid, rpm.userid) AS userid,
                COALESCE(cm.total_cards, 0) AS total_cards,
                COALESCE(cm.total_unique_cards, 0) AS total_unique_cards,
                COALESCE(cm.unique_cards_with_variant, 0) AS unique_cards_with_variant,
                COALESCE(cm.total_value, 0) AS total_value,
                COALESCE(cm.graded, 0) AS graded,
                COALESCE(cm.sold, 0) AS sold,
                COALESCE(tm.purchases, 0) AS purchases,
                COALESCE(tm.other_costs, 0) AS other_costs,
                COALESCE(tm.sales, 0) AS sales,
                -- Calculate total profit (sales - purchases - other_costs)
                COALESCE(tm.sales, 0) - COALESCE(tm.purchases, 0) - COALESCE(tm.other_costs, 0) AS profit,
                COALESCE(rpm.realized_profit, 0) AS realized_profit,
                COALESCE(cm.unrealized_profit_from_cards, 0) AS unrealized_profit,
                CURRENT_DATE AS log_date
            FROM card_metrics cm
            FULL OUTER JOIN transaction_metrics tm ON cm.userid = tm.userid
            FULL OUTER JOIN realized_profit_metrics rpm ON COALESCE(cm.userid, tm.userid) = rpm.userid
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

    if (errors == 0) {
        result.state = true;
        return Promise.resolve(result)
    } else {
        result.state = false;
        return Promise.reject(result)
    }

}