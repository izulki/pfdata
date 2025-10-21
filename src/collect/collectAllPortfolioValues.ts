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
                cards_value,
                graded,
                cards_sold,
                total_sealed_products,
                total_unique_sealed_products,
                sealed_value,
                sealed_sold,
                total_value,
                cards_purchases,
                cards_other_costs,
                cards_sales,
                sealed_purchases,
                sealed_other_costs,
                sealed_sales,
                total_purchases,
                total_other_costs,
                total_sales,
                total_profit,
                cards_realized_profit,
                sealed_realized_profit,
                total_realized_profit,
                cards_unrealized_profit,
                sealed_unrealized_profit,
                total_unrealized_profit,
                log_date
            )
            WITH card_metrics AS (
                -- Calculate card counts and values without transaction joins to avoid duplication
                SELECT 
                    uc.userid,
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
                    SUM(CASE 
                        WHEN pucid.status < 100 OR pucid.status IS NULL 
                        THEN COALESCE(graded_prices.current_price, raw_prices.current_price, 0)
                    END) AS cards_value,
                    COUNT(CASE 
                        WHEN (pucid.status < 100 OR pucid.status IS NULL)
                        AND pucid.grade IS NOT NULL 
                        AND pucid.grade != 'Ungraded' 
                        THEN 1 
                    END) AS graded,
                    COUNT(CASE 
                        WHEN pucid.status >= 100 
                        THEN 1 
                    END) AS cards_sold
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_card_inventory_details pucid 
                    ON pucid.inventory_id = uc.id 
                LEFT JOIN pfanalysis_graded_price_changes_daily graded_prices 
                    ON uc.cardid = graded_prices.cardid 
                    AND uc.variant = graded_prices.variant 
                    AND pucid.grade = graded_prices.grade
                    AND pucid.grade IS NOT NULL 
                    AND pucid.grade != 'Ungraded'
                LEFT JOIN pfanalysis_price_changes_daily raw_prices 
                    ON uc.cardid = raw_prices.cardid 
                    AND uc.variant = raw_prices.variant
                    AND (pucid.grade IS NULL OR pucid.grade = 'Ungraded' OR graded_prices.current_price IS NULL)
                GROUP BY uc.userid
            ),
            card_cost_basis AS (
                -- Calculate card cost basis from transactions separately to avoid duplication
                SELECT 
                    uc.userid,
                    SUM(CASE 
                        WHEN (pucid.status < 100 OR pucid.status IS NULL)
                        AND uit.transaction_type = 100
                        THEN COALESCE(uit.amount_usd, 0)
                    END) AS owned_cards_cost_basis
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_card_inventory_details pucid 
                    ON pucid.inventory_id = uc.id 
                LEFT JOIN pf_users_inventory_transactions uit 
                    ON uit.inventory_id = uc.id
                GROUP BY uc.userid
            ),
            sealed_metrics AS (
                -- Calculate sealed product counts and values without transaction joins
                SELECT 
                    pusi.userid,
                    COUNT(CASE 
                        WHEN pusid.status < 100 OR pusid.status IS NULL 
                        THEN 1 
                    END) AS total_sealed_products,
                    COUNT(DISTINCT CASE 
                        WHEN pusid.status < 100 OR pusid.status IS NULL 
                        THEN pusi.sealedid 
                    END) AS total_unique_sealed_products,
                    SUM(CASE 
                        WHEN pusid.status < 100 OR pusid.status IS NULL 
                        THEN COALESCE(pspcd.current_price, 0)
                    END) AS sealed_value,
                    COUNT(CASE 
                        WHEN pusid.status >= 100 
                        THEN 1 
                    END) AS sealed_sold
                FROM pf_users_sealed_inventory pusi
                LEFT JOIN pf_users_sealed_inventory_details pusid 
                    ON pusi.uuid = pusid.inventory_uuid
                LEFT JOIN pfanalysis_sealed_price_changes_daily pspcd
                    ON pusi.sealedid = pspcd.sealedid
                GROUP BY pusi.userid
            ),
            sealed_cost_basis AS (
                -- Calculate sealed cost basis from transactions separately
                SELECT 
                    pusi.userid,
                    SUM(CASE 
                        WHEN (pusid.status < 100 OR pusid.status IS NULL)
                        AND pusit.transaction_type = 100
                        THEN COALESCE(pusit.amount_usd, 0)
                    END) AS owned_sealed_cost_basis
                FROM pf_users_sealed_inventory pusi
                LEFT JOIN pf_users_sealed_inventory_details pusid 
                    ON pusi.uuid = pusid.inventory_uuid
                LEFT JOIN pf_users_sealed_inventory_transactions pusit
                    ON pusit.inventory_uuid = pusi.uuid
                GROUP BY pusi.userid
            ),
            transaction_metrics AS (
                -- Calculate card transaction totals
                SELECT 
                    uc.userid,
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type = 100 
                        THEN uit.amount_usd 
                    END), 0) AS purchases,
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type IN (101, 102, 103, 104) 
                        THEN uit.amount_usd 
                    END), 0) AS other_costs,
                    COALESCE(SUM(CASE 
                        WHEN uit.transaction_type IN (200, 201) 
                        THEN uit.amount_usd 
                    END), 0) AS sales
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_inventory_transactions uit 
                    ON uit.inventory_id = uc.id
                GROUP BY uc.userid
            ),
            sealed_transaction_metrics AS (
                -- Calculate sealed transaction totals
                SELECT 
                    pusi.userid,
                    COALESCE(SUM(CASE 
                        WHEN pusit.transaction_type = 100 
                        THEN pusit.amount_usd 
                    END), 0) AS sealed_purchases,
                    COALESCE(SUM(CASE 
                        WHEN pusit.transaction_type IN (101, 102, 103, 104) 
                        THEN pusit.amount_usd 
                    END), 0) AS sealed_other_costs,
                    COALESCE(SUM(CASE 
                        WHEN pusit.transaction_type IN (200, 201) 
                        THEN pusit.amount_usd 
                    END), 0) AS sealed_sales
                FROM pf_users_sealed_inventory pusi
                LEFT JOIN pf_users_sealed_inventory_transactions pusit 
                    ON pusit.inventory_uuid = pusi.uuid
                GROUP BY pusi.userid
            ),
            realized_profit_metrics AS (
                -- Calculate realized profit from sold cards only
                SELECT 
                    uc.userid,
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
                    END), 0) AS cards_realized_profit
                FROM pf_users_card_inventory uc
                LEFT JOIN pf_users_card_inventory_details pucid 
                    ON pucid.inventory_id = uc.id 
                LEFT JOIN pf_users_inventory_transactions uit 
                    ON uit.inventory_id = uc.id
                GROUP BY uc.userid
            ),
            sealed_realized_profit_metrics AS (
                -- Calculate realized profit from sold sealed products only
                SELECT 
                    pusi.userid,
                    COALESCE(SUM(CASE 
                        WHEN pusid.status >= 100 AND pusit.transaction_type IN (200, 201) 
                        THEN pusit.amount_usd 
                    END), 0) - 
                    COALESCE(SUM(CASE 
                        WHEN pusid.status >= 100 AND pusit.transaction_type = 100 
                        THEN pusit.amount_usd 
                    END), 0) - 
                    COALESCE(SUM(CASE 
                        WHEN pusid.status >= 100 AND pusit.transaction_type IN (101, 102, 103, 104) 
                        THEN pusit.amount_usd 
                    END), 0) AS sealed_realized_profit
                FROM pf_users_sealed_inventory pusi
                LEFT JOIN pf_users_sealed_inventory_details pusid 
                    ON pusi.uuid = pusid.inventory_uuid
                LEFT JOIN pf_users_sealed_inventory_transactions pusit 
                    ON pusit.inventory_uuid = pusi.uuid
                GROUP BY pusi.userid
            )
            SELECT 
                COALESCE(cm.userid, ccb.userid, sm.userid, scb.userid, tm.userid, stm.userid, rpm.userid, srpm.userid) AS userid,
                -- Card metrics
                COALESCE(cm.total_cards, 0) AS total_cards,
                COALESCE(cm.total_unique_cards, 0) AS total_unique_cards,
                COALESCE(cm.unique_cards_with_variant, 0) AS unique_cards_with_variant,
                COALESCE(cm.cards_value, 0) AS cards_value,
                COALESCE(cm.graded, 0) AS graded,
                COALESCE(cm.cards_sold, 0) AS cards_sold,
                -- Sealed metrics
                COALESCE(sm.total_sealed_products, 0) AS total_sealed_products,
                COALESCE(sm.total_unique_sealed_products, 0) AS total_unique_sealed_products,
                COALESCE(sm.sealed_value, 0) AS sealed_value,
                COALESCE(sm.sealed_sold, 0) AS sealed_sold,
                -- Combined value
                COALESCE(cm.cards_value, 0) + COALESCE(sm.sealed_value, 0) AS total_value,
                -- Card transaction metrics
                COALESCE(tm.purchases, 0) AS cards_purchases,
                COALESCE(tm.other_costs, 0) AS cards_other_costs,
                COALESCE(tm.sales, 0) AS cards_sales,
                -- Sealed transaction metrics
                COALESCE(stm.sealed_purchases, 0) AS sealed_purchases,
                COALESCE(stm.sealed_other_costs, 0) AS sealed_other_costs,
                COALESCE(stm.sealed_sales, 0) AS sealed_sales,
                -- Combined transaction metrics
                COALESCE(tm.purchases, 0) + COALESCE(stm.sealed_purchases, 0) AS total_purchases,
                COALESCE(tm.other_costs, 0) + COALESCE(stm.sealed_other_costs, 0) AS total_other_costs,
                COALESCE(tm.sales, 0) + COALESCE(stm.sealed_sales, 0) AS total_sales,
                -- Combined profit (net cash flow)
                (COALESCE(tm.sales, 0) + COALESCE(stm.sealed_sales, 0)) - 
                (COALESCE(tm.purchases, 0) + COALESCE(stm.sealed_purchases, 0)) - 
                (COALESCE(tm.other_costs, 0) + COALESCE(stm.sealed_other_costs, 0)) AS total_profit,
                -- Realized profit (from sold items only)
                COALESCE(rpm.cards_realized_profit, 0) AS cards_realized_profit,
                COALESCE(srpm.sealed_realized_profit, 0) AS sealed_realized_profit,
                COALESCE(rpm.cards_realized_profit, 0) + COALESCE(srpm.sealed_realized_profit, 0) AS total_realized_profit,
                -- Unrealized profit (current value minus cost basis for owned items)
                COALESCE(cm.cards_value, 0) - COALESCE(ccb.owned_cards_cost_basis, 0) AS cards_unrealized_profit,
                COALESCE(sm.sealed_value, 0) - COALESCE(scb.owned_sealed_cost_basis, 0) AS sealed_unrealized_profit,
                (COALESCE(cm.cards_value, 0) - COALESCE(ccb.owned_cards_cost_basis, 0)) + 
                (COALESCE(sm.sealed_value, 0) - COALESCE(scb.owned_sealed_cost_basis, 0)) AS total_unrealized_profit,
                -- Log timestamp
                CURRENT_DATE AS log_date
            FROM card_metrics cm
            FULL OUTER JOIN card_cost_basis ccb ON cm.userid = ccb.userid
            FULL OUTER JOIN sealed_metrics sm ON COALESCE(cm.userid, ccb.userid) = sm.userid
            FULL OUTER JOIN sealed_cost_basis scb ON COALESCE(cm.userid, ccb.userid, sm.userid) = scb.userid
            FULL OUTER JOIN transaction_metrics tm ON COALESCE(cm.userid, ccb.userid, sm.userid, scb.userid) = tm.userid
            FULL OUTER JOIN sealed_transaction_metrics stm ON COALESCE(cm.userid, ccb.userid, sm.userid, scb.userid, tm.userid) = stm.userid
            FULL OUTER JOIN realized_profit_metrics rpm ON COALESCE(cm.userid, ccb.userid, sm.userid, scb.userid, tm.userid, stm.userid) = rpm.userid
            FULL OUTER JOIN sealed_realized_profit_metrics srpm ON COALESCE(cm.userid, ccb.userid, sm.userid, scb.userid, tm.userid, stm.userid, rpm.userid) = srpm.userid
            -- Prevent duplicate entries for same user on same day
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