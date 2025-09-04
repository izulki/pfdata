const pgp = require('pg-promise')(); //User Postgres Helpers

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface CollectAnalysisResponse {
    state: boolean,
    errors: number,
    log: number
}

// Initialize the process
// Initialize the process
async function initializeGradedPriceChangesProcess(db: any): Promise<boolean> {
    try {
        // 1. Drop the staging table if it exists
        await db.none(`DROP TABLE IF EXISTS pfanalysis_staging_graded_price_changes_daily`);

        // 2. Create staging table by copying structure from the main table
        await db.none(`
          CREATE TABLE pfanalysis_staging_graded_price_changes_daily AS
          SELECT * FROM pfanalysis_graded_price_changes_daily
          WHERE 1=0
      `);

        // 3. Add new sets to tracking if they don't exist
        await db.none(`
          INSERT INTO pfanalysis_tracking_graded_price_changes_daily (setid, status)
          SELECT ps.setid, 'PENDING'
          FROM pfdata_sets ps
          WHERE NOT EXISTS (
              SELECT 1 FROM pfanalysis_tracking_graded_price_changes_daily pt
              WHERE pt.setid = ps.setid
          )
      `);

        // 4. Reset status for all sets to pending
        await db.none(`
          UPDATE pfanalysis_tracking_graded_price_changes_daily
          SET status = 'PENDING', 
              processed_at = NULL,
              record_count = 0
      `);

        return true;
    } catch (error) {
        console.error("Error initializing price changes process:", error);
        return false;
    }
}

// Process a single set
async function processGradedPriceChangesForSet(db: any, setid: string, logger: any): Promise<number> {
    try {
        // Update this set's status to IN_PROGRESS
        await db.none(`
            UPDATE pfanalysis_tracking_graded_price_changes_daily
            SET status = 'IN_PROGRESS', processed_at = NOW()
            WHERE setid = $1
        `, [setid]);

        logger.info(`Processing price changes for set: ${setid}`);

        // Process this set and insert into staging
        const result = await db.one(`
                current_prices AS (
                    WITH latest_dates_per_card AS (
                        SELECT cardid, variant, grade, MAX(sold_date::date) as latest_date
                        FROM pf_graded_cards_price_history t
                        LEFT JOIN pfdata_cards pc ON t.cardid = pc.cardid
                        WHERE pc.setid = $1 AND t.sold_date IS NOT NULL
                        GROUP BY cardid, variant, grade
                    )
                    SELECT 
                        t.cardid, t.variant, t.grade,
                        AVG(t.price) as price,  -- Average of all sales on latest date
                        MAX(t.sold_date) as sold_date
                    FROM pf_graded_cards_price_history t
                    JOIN latest_dates_per_card ld 
                        ON t.cardid = ld.cardid 
                        AND t.variant = ld.variant 
                        AND t.grade = ld.grade
                        AND t.sold_date::date = ld.latest_date
                    GROUP BY t.cardid, t.variant, t.grade
            ),
            -- Window-based prices (preferred method)
            window_interval_prices AS (
                SELECT 
                    t.cardid,
                    t.variant,
                    t.grade,
                    interval_type,
                    AVG(ph.price) as interval_price
                FROM current_prices t
                CROSS JOIN (VALUES 
                    ('1w', INTERVAL '7 days', INTERVAL '3 days'),
                    ('1m', INTERVAL '1 month', INTERVAL '5 days'),
                    ('3m', INTERVAL '3 months', INTERVAL '10 days'),
                    ('6m', INTERVAL '6 months', INTERVAL '15 days'),
                    ('1y', INTERVAL '1 year', INTERVAL '30 days')
                ) as intervals(interval_type, target_interval, tolerance_window)
                LEFT JOIN pf_graded_cards_price_history ph
                    ON t.cardid = ph.cardid
                    AND t.variant = ph.variant
                    AND t.grade = ph.grade
                    AND ph.sold_date::date <= t.sold_date::date - (target_interval - tolerance_window)
                    AND ph.sold_date::date >= t.sold_date::date - (target_interval + tolerance_window)
                WHERE ph.price IS NOT NULL
                GROUP BY t.cardid, t.variant, t.grade, interval_type
            ),
            -- Fallback prices (last known price before target date)
            fallback_interval_prices AS (
                SELECT DISTINCT ON (t.cardid, t.variant, t.grade, interval_type)
                    t.cardid,
                    t.variant,
                    t.grade,
                    interval_type,
                    ph.price as fallback_price
                FROM current_prices t
                CROSS JOIN (VALUES 
                    ('1w', INTERVAL '7 days'),
                    ('1m', INTERVAL '1 month'),
                    ('3m', INTERVAL '3 months'),
                    ('6m', INTERVAL '6 months'),
                    ('1y', INTERVAL '1 year')
                ) as intervals(interval_type, target_interval)
                LEFT JOIN pf_graded_cards_price_history ph
                    ON t.cardid = ph.cardid
                    AND t.variant = ph.variant
                    AND t.grade = ph.grade
                    AND ph.sold_date::date <= t.sold_date::date - target_interval
                WHERE ph.price IS NOT NULL
                ORDER BY t.cardid, t.variant, t.grade, interval_type, ph.sold_date DESC
            ),
            -- Combined interval prices with fallback logic
            final_interval_prices AS (
                SELECT 
                    COALESCE(w.cardid, f.cardid) as cardid,
                    COALESCE(w.variant, f.variant) as variant,
                    COALESCE(w.grade, f.grade) as grade,
                    COALESCE(w.interval_type, f.interval_type) as interval_type,
                    COALESCE(w.interval_price, f.fallback_price) as final_price,
                    CASE 
                        WHEN w.interval_price IS NOT NULL THEN 'WINDOW'
                        WHEN f.fallback_price IS NOT NULL THEN 'FALLBACK'
                        ELSE 'NO_DATA'
                    END as price_source
                FROM window_interval_prices w
                FULL OUTER JOIN fallback_interval_prices f
                    ON w.cardid = f.cardid 
                    AND w.variant = f.variant 
                    AND w.grade = f.grade 
                    AND w.interval_type = f.interval_type
            ),
            -- Window-based previous day prices
            window_previous_prices AS (
                SELECT 
                    t.cardid,
                    t.variant,
                    t.grade,
                    AVG(ph.price) as previous_price
                FROM current_prices t
                LEFT JOIN pf_graded_cards_price_history ph
                    ON t.cardid = ph.cardid
                    AND t.variant = ph.variant
                    AND t.grade = ph.grade
                    AND ph.sold_date::date < t.sold_date::date
                    AND ph.sold_date::date >= t.sold_date::date - INTERVAL '7 days'
                WHERE ph.price IS NOT NULL
                GROUP BY t.cardid, t.variant, t.grade
            ),
            -- Fallback previous day prices
            fallback_previous_prices AS (
                SELECT DISTINCT ON (t.cardid, t.variant, t.grade)
                    t.cardid,
                    t.variant,
                    t.grade,
                    ph.price as fallback_previous_price
                FROM current_prices t
                LEFT JOIN pf_graded_cards_price_history ph
                    ON t.cardid = ph.cardid
                    AND t.variant = ph.variant
                    AND t.grade = ph.grade
                    AND ph.sold_date::date < t.sold_date::date
                WHERE ph.price IS NOT NULL
                ORDER BY t.cardid, t.variant, t.grade, ph.sold_date DESC
            ),
            -- Combined previous prices
            final_previous_prices AS (
                SELECT 
                    COALESCE(w.cardid, f.cardid) as cardid,
                    COALESCE(w.variant, f.variant) as variant,
                    COALESCE(w.grade, f.grade) as grade,
                    COALESCE(w.previous_price, f.fallback_previous_price) as final_previous_price,
                    CASE 
                        WHEN w.previous_price IS NOT NULL THEN 'WINDOW'
                        WHEN f.fallback_previous_price IS NOT NULL THEN 'FALLBACK'
                        ELSE 'NO_DATA'
                    END as previous_price_source
                FROM window_previous_prices w
                FULL OUTER JOIN fallback_previous_prices f
                    ON w.cardid = f.cardid 
                    AND w.variant = f.variant 
                    AND w.grade = f.grade
            ),
            inserted AS (
                INSERT INTO pfanalysis_staging_graded_price_changes_daily
                SELECT 
                    -- Primary key columns
                    t.cardid,
                    t.variant,
                    t.grade,
                    t.price as current_price,
                    fpp.final_previous_price as previous_price,
                    ((t.price - fpp.final_previous_price) / NULLIF(fpp.final_previous_price, 0)) * 100 as percentage_change,
                    t.sold_date::date as latest_update_date,
                    t.sold_date::date - INTERVAL '1 day' as previous_update_date, -- Simplified since we don't know exact date
                    -- Weekly changes
                    p1w.final_price as previous_price_1w,
                    (t.price - p1w.final_price) as price_change_1w,
                    ((t.price - p1w.final_price) / NULLIF(p1w.final_price, 0)) * 100 as percentage_change_1w,
                    -- Monthly changes  
                    p1m.final_price as previous_price_1m,
                    (t.price - p1m.final_price) as price_change_1m,
                    ((t.price - p1m.final_price) / NULLIF(p1m.final_price, 0)) * 100 as percentage_change_1m,
                    -- 3 Month changes
                    p3m.final_price as previous_price_3m,
                    (t.price - p3m.final_price) as price_change_3m,
                    ((t.price - p3m.final_price) / NULLIF(p3m.final_price, 0)) * 100 as percentage_change_3m,
                    -- 6 Month changes
                    p6m.final_price as previous_price_6m,
                    (t.price - p6m.final_price) as price_change_6m,
                    ((t.price - p6m.final_price) / NULLIF(p6m.final_price, 0)) * 100 as percentage_change_6m,
                    -- Yearly changes
                    p1y.final_price as previous_price_1y,
                    (t.price - p1y.final_price) as price_change_1y,
                    ((t.price - p1y.final_price) / NULLIF(p1y.final_price, 0)) * 100 as percentage_change_1y,
                    -- Data source tracking (if you add these columns)
                    fpp.previous_price_source as price_source_previous,
                    p1w.price_source as price_source_1w,
                    p1m.price_source as price_source_1m,
                    p3m.price_source as price_source_3m,
                    p6m.price_source as price_source_6m,
                    p1y.price_source as price_source_1y
                FROM current_prices t
                LEFT JOIN final_previous_prices fpp
                    ON t.cardid = fpp.cardid 
                    AND t.variant = fpp.variant
                    AND t.grade = fpp.grade
                LEFT JOIN final_interval_prices p1w
                    ON t.cardid = p1w.cardid 
                    AND t.variant = p1w.variant 
                    AND t.grade = p1w.grade
                    AND p1w.interval_type = '1w'
                LEFT JOIN final_interval_prices p1m
                    ON t.cardid = p1m.cardid 
                    AND t.variant = p1m.variant 
                    AND t.grade = p1m.grade
                    AND p1m.interval_type = '1m'
                LEFT JOIN final_interval_prices p3m
                    ON t.cardid = p3m.cardid 
                    AND t.variant = p3m.variant 
                    AND t.grade = p3m.grade
                    AND p3m.interval_type = '3m'
                LEFT JOIN final_interval_prices p6m
                    ON t.cardid = p6m.cardid 
                    AND t.variant = p6m.variant 
                    AND t.grade = p6m.grade
                    AND p6m.interval_type = '6m'
                LEFT JOIN final_interval_prices p1y
                    ON t.cardid = p1y.cardid 
                    AND t.variant = p1y.variant 
                    AND t.grade = p1y.grade
                    AND p1y.interval_type = '1y'
                RETURNING 1
            )
            SELECT COUNT(*) as count FROM inserted
        `, [setid]);

        // Update status to COMPLETED with record count - FIX: Use correct table name
        await db.none(`
            UPDATE pfanalysis_tracking_graded_price_changes_daily
            SET status = 'COMPLETED', 
                processed_at = NOW(),
                record_count = $1
            WHERE setid = $2
        `, [result.count, setid]);

        logger.info(`Completed set ${setid} with ${result.count} records`);
        return result.count;
    } catch (error) {
        // Update status to FAILED - FIX: Use correct table name
        await db.none(`
            UPDATE pfanalysis_tracking_graded_price_changes_daily
            SET status = 'FAILED', 
                processed_at = NOW()
            WHERE setid = $1
        `, [setid]);

        logger.error(`Failed processing set ${setid}: ${error.message}`);
        throw error;
    }
}

// Check if all sets are completed
async function areAllSetsProcessed(db: any): Promise<boolean> {
    const result = await db.one(`
        SELECT 
            (SELECT COUNT(*) FROM pfanalysis_tracking_graded_price_changes_daily) as total,
            (SELECT COUNT(*) FROM pfanalysis_tracking_graded_price_changes_daily WHERE status = 'COMPLETED') as completed
    `);

    return result.total === result.completed;
}

// Finalize the update
async function finalizePriceChangesUpdate(db: any, logger: any): Promise<boolean> {
    try {
        // Check if all sets are processed
        const allProcessed = await areAllSetsProcessed(db);
        if (!allProcessed) {
            logger.warning("Not all sets are processed. Cannot finalize update.");
            return false;
        }

        logger.info("All sets processed. Finalizing update to main table.");

        // Use a transaction for the final update
        await db.tx(async (t: any) => {
            await t.none(`TRUNCATE TABLE pfanalysis_graded_price_changes_daily`);
            await t.none(`
                INSERT INTO pfanalysis_graded_price_changes_daily
                SELECT * FROM pfanalysis_staging_graded_price_changes_daily
            `);
        });

        logger.info("Successfully updated main table.");
        return true;
    } catch (error) {
        logger.error(`Failed to finalize update: ${error.message}`);
        return false;
    }
}

// Clean up after successful completion
async function cleanupPriceChangesProcess(db: any, logger: any): Promise<boolean> {
    try {
        logger.info("Cleaning up staging and tracking tables");

        // Clean up the staging table
        await db.none(`TRUNCATE TABLE pfanalysis_staging_graded_price_changes_daily`);

        // Reset the tracking table statuses but keep the entries for future runs
        await db.none(`
            UPDATE pfanalysis_tracking_graded_price_changes_daily
            SET status = 'COMPLETED',
                processed_at = NOW()
        `);

        logger.info("Cleanup completed");
        return true;
    } catch (error) {
        logger.error(`Failed to clean up: ${error.message}`);
        return false;
    }
}

// Main function to coordinate the process
export default async function CollectGradedAnalysis(db: any, method: string): Promise<CollectAnalysisResponse> {
    let state = false;
    let errors = 0;

    // Initialize logging
    let logid = await logToDBStart(db, "collectGradedAnalysis", method);
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectGradedAnalysis' }),
            timestamp(),
            json()
        ),
        transports: [
            new transports.File({ filename: `logs/collectGradedAnalysis/${logid}.log` }),
        ],
    });

    logger.info("--- STARTING GRADED PRICE CHANGES ANALYSIS ---");

    try {
        // Initialize the process
        logger.info("Initializing price changes process");
        await initializeGradedPriceChangesProcess(db);

        // Get pending sets
        logger.info("Getting list of sets to process");
        const pendingSets = await db.any(`
            SELECT setid FROM pfanalysis_tracking_graded_price_changes_daily 
            WHERE status = 'PENDING'
            ORDER BY setid
        `);

        logger.info(`Found ${pendingSets.length} sets to process`);

        // Process each set
        for (const set of pendingSets) {
            try {
                await processGradedPriceChangesForSet(db, set.setid, logger);
            } catch (error) {
                errors++;
                logger.error(`Error processing set ${set.setid}: ${error.message}`);
                // Continue with other sets even if one fails
            }
        }

        // Check if all sets processed successfully
        const allProcessed = await areAllSetsProcessed(db);

        // Finalize if all sets processed
        if (allProcessed) {
            logger.info("All sets processed successfully. Finalizing update.");
            const finalized = await finalizePriceChangesUpdate(db, logger);
            state = finalized;

            // If finalized successfully, clean up
            if (finalized) {
                await cleanupPriceChangesProcess(db, logger);
            }
        } else {
            // Report sets that failed
            const failedSets = await db.any(`
                SELECT setid FROM pfanalysis_tracking_graded_price_changes_daily
                WHERE status = 'FAILED'
                ORDER BY setid
            `);

            logger.warning(`${failedSets.length} sets failed processing: ${failedSets.map(s => s.setid).join(', ')}`);
            logger.warning("Not finalizing update due to failed sets");
            state = false;
        }
    } catch (error) {
        errors++;
        logger.error(`Unexpected error in CollectGradedAnalysis: ${error.message}`);
        state = false;
    }

    await logToDBEnd(db, logid, state ? "COMPLETED" : "FAILED", errors, `/logs/collectGradedAnalysis/${logid}.log`);
    logger.info("--- GRADED PRICE CHANGES ANALYSIS COMPLETED ---");

    return {
        state: state,
        errors: errors,
        log: logid
    };
}