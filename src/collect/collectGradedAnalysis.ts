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
            WITH latest_date AS (
                SELECT updated::date as date
                FROM pf_graded_cards_price_history
                WHERE updated IS NOT NULL
                ORDER BY updated DESC
                LIMIT 1
            ),
            current_prices AS (
                SELECT DISTINCT ON (t.cardid, t.variant, t.grade)
                    t.cardid, t.variant, t.grade, t.price, t.updated
                FROM pf_graded_cards_price_history t
                JOIN latest_date ld ON t.updated::date = ld.date
                LEFT JOIN pfdata_cards pc ON t.cardid = pc.cardid
                WHERE pc.setid = $1
                ORDER BY t.cardid, t.variant, t.grade, t.updated DESC
            ),
            interval_prices AS (
                -- First, find the closest date for each interval
                WITH closest_dates AS (
                    SELECT DISTINCT ON (t.cardid, t.variant, t.grade, interval_type)
                        t.cardid,
                        t.variant,
                        t.grade,
                        interval_type,
                        ph.updated::date as closest_date
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
                        AND ph.updated::date <= t.updated::date - (target_interval - tolerance_window)
                        AND ph.updated::date >= t.updated::date - (target_interval + tolerance_window)
                    WHERE ph.price IS NOT NULL
                    ORDER BY t.cardid, t.variant, t.grade, interval_type, 
                            ABS(EXTRACT(EPOCH FROM (ph.updated::date - (t.updated::date - target_interval)))) ASC
                )
                -- Then average all prices from that closest date
                SELECT 
                    cd.cardid,
                    cd.variant,
                    cd.grade,
                    cd.interval_type,
                    AVG(ph.price) as interval_price
                FROM closest_dates cd
                LEFT JOIN pf_graded_cards_price_history ph
                    ON cd.cardid = ph.cardid
                    AND cd.variant = ph.variant
                    AND cd.grade = ph.grade
                    AND ph.updated::date = cd.closest_date
                WHERE ph.price IS NOT NULL
                GROUP BY cd.cardid, cd.variant, cd.grade, cd.interval_type
            ),
            previous_day_prices AS (
                SELECT DISTINCT ON (t.cardid, t.variant, t.grade)
                    t.cardid,
                    t.variant,
                    t.grade,
                    ph.price as previous_price,
                    ph.updated::date as previous_date
                FROM current_prices t
                LEFT JOIN pf_graded_cards_price_history ph
                    ON t.cardid = ph.cardid
                    AND t.variant = ph.variant
                    AND t.grade = ph.grade
                    AND ph.updated::date < t.updated::date
                    AND ph.updated::date >= t.updated::date - INTERVAL '7 days'
                ORDER BY t.cardid, t.variant, t.grade, ph.updated DESC
            ),
            inserted AS (
                INSERT INTO pfanalysis_staging_graded_price_changes_daily
                SELECT 
                    -- Primary key columns
                    t.cardid,
                    t.variant,
                    t.grade,
                    t.price as current_price,
                    pdp.previous_price as previous_price,
                    ((t.price - pdp.previous_price) / NULLIF(pdp.previous_price, 0)) * 100 as percentage_change,
                    t.updated::date as latest_update_date,
                    pdp.previous_date as previous_update_date,
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
                FROM current_prices t
                LEFT JOIN previous_day_prices pdp
                    ON t.cardid = pdp.cardid 
                    AND t.variant = pdp.variant
                    AND t.grade = pdp.grade
                LEFT JOIN interval_prices p1w
                    ON t.cardid = p1w.cardid 
                    AND t.variant = p1w.variant 
                    AND t.grade = p1w.grade
                    AND p1w.interval_type = '1w'
                LEFT JOIN interval_prices p1m
                    ON t.cardid = p1m.cardid 
                    AND t.variant = p1m.variant 
                    AND t.grade = p1m.grade
                    AND p1m.interval_type = '1m'
                LEFT JOIN interval_prices p3m
                    ON t.cardid = p3m.cardid 
                    AND t.variant = p3m.variant 
                    AND t.grade = p3m.grade
                    AND p3m.interval_type = '3m'
                LEFT JOIN interval_prices p6m
                    ON t.cardid = p6m.cardid 
                    AND t.variant = p6m.variant 
                    AND t.grade = p6m.grade
                    AND p6m.interval_type = '6m'
                LEFT JOIN interval_prices p1y
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