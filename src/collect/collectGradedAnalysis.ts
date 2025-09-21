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
            WITH current_prices AS (
                    SELECT DISTINCT ON (t.cardid, t.variant, t.grade)
                        t.cardid, 
                        t.variant, 
                        t.grade,
                        t.price as current_price,
                        t.sold_date::timestamp as latest_date
                    FROM pf_graded_cards_price_history t
                    LEFT JOIN pfdata_cards pc ON t.cardid = pc.cardid
                    WHERE pc.setid = $1 
                        AND t.sold_date IS NOT NULL 
                        AND t.price IS NOT NULL
                    ORDER BY t.cardid, t.variant, t.grade, t.sold_date::timestamp DESC, t.id DESC
                ),
                
                previous_day_prices AS (
                    SELECT DISTINCT ON (cp.cardid, cp.variant, cp.grade)
                        cp.cardid, cp.variant, cp.grade,
                        ph.price as previous_price,
                        ph.sold_date::timestamp as previous_date
                    FROM current_prices cp
                    LEFT JOIN pf_graded_cards_price_history ph 
                        ON cp.cardid = ph.cardid 
                        AND cp.variant = ph.variant 
                        AND cp.grade = ph.grade
                        AND ph.sold_date::timestamp < cp.latest_date
                        AND ph.price IS NOT NULL
                    ORDER BY cp.cardid, cp.variant, cp.grade, ph.sold_date::timestamp DESC, ph.id DESC
                ),
                
                interval_prices AS (
                    SELECT 
                        cp.cardid, cp.variant, cp.grade,
                        (SELECT ph.price FROM pf_graded_cards_price_history ph 
                        WHERE ph.cardid = cp.cardid AND ph.variant = cp.variant AND ph.grade = cp.grade
                        AND ph.sold_date::timestamp <= cp.latest_date - INTERVAL '7 days' AND ph.price IS NOT NULL
                        ORDER BY ABS(EXTRACT(EPOCH FROM (cp.latest_date - INTERVAL '7 days' - ph.sold_date::timestamp)))
                        LIMIT 1) as price_1w,
                        (SELECT ph.price FROM pf_graded_cards_price_history ph 
                        WHERE ph.cardid = cp.cardid AND ph.variant = cp.variant AND ph.grade = cp.grade
                        AND ph.sold_date::timestamp <= cp.latest_date - INTERVAL '1 month' AND ph.price IS NOT NULL
                        ORDER BY ABS(EXTRACT(EPOCH FROM (cp.latest_date - INTERVAL '1 month' - ph.sold_date::timestamp)))
                        LIMIT 1) as price_1m,
                        (SELECT ph.price FROM pf_graded_cards_price_history ph 
                        WHERE ph.cardid = cp.cardid AND ph.variant = cp.variant AND ph.grade = cp.grade
                        AND ph.sold_date::timestamp <= cp.latest_date - INTERVAL '3 months' AND ph.price IS NOT NULL
                        ORDER BY ABS(EXTRACT(EPOCH FROM (cp.latest_date - INTERVAL '3 months' - ph.sold_date::timestamp)))
                        LIMIT 1) as price_3m,
                        (SELECT ph.price FROM pf_graded_cards_price_history ph 
                        WHERE ph.cardid = cp.cardid AND ph.variant = cp.variant AND ph.grade = cp.grade
                        AND ph.sold_date::timestamp <= cp.latest_date - INTERVAL '6 months' AND ph.price IS NOT NULL
                        ORDER BY ABS(EXTRACT(EPOCH FROM (cp.latest_date - INTERVAL '6 months' - ph.sold_date::timestamp)))
                        LIMIT 1) as price_6m,
                        (SELECT ph.price FROM pf_graded_cards_price_history ph 
                        WHERE ph.cardid = cp.cardid AND ph.variant = cp.variant AND ph.grade = cp.grade
                        AND ph.sold_date::timestamp <= cp.latest_date - INTERVAL '1 year' AND ph.price IS NOT NULL
                        ORDER BY ABS(EXTRACT(EPOCH FROM (cp.latest_date - INTERVAL '1 year' - ph.sold_date::timestamp)))
                        LIMIT 1) as price_1y
                    FROM current_prices cp
                ),
                
                inserted AS (
                    INSERT INTO pfanalysis_staging_graded_price_changes_daily (
                        cardid, variant, grade, current_price, previous_price, percentage_change,
                        latest_update_date, previous_update_date,
                        previous_price_1w, price_change_1w, percentage_change_1w,
                        previous_price_1m, price_change_1m, percentage_change_1m,
                        previous_price_3m, price_change_3m, percentage_change_3m,
                        previous_price_6m, price_change_6m, percentage_change_6m,
                        previous_price_1y, price_change_1y, percentage_change_1y
                    )
                    SELECT 
                        cp.cardid, cp.variant, cp.grade, cp.current_price, pdp.previous_price,
                        CASE WHEN pdp.previous_price IS NOT NULL AND pdp.previous_price > 0 
                            THEN ((cp.current_price - pdp.previous_price) / pdp.previous_price) * 100
                            ELSE NULL END as percentage_change,
                        cp.latest_date::date, pdp.previous_date::date,
                        
                        ip.price_1w,
                        CASE WHEN ip.price_1w IS NOT NULL THEN cp.current_price - ip.price_1w ELSE NULL END,
                        CASE WHEN ip.price_1w IS NOT NULL AND ip.price_1w > 0 
                            THEN ((cp.current_price - ip.price_1w) / ip.price_1w) * 100 ELSE NULL END,
                            
                        ip.price_1m,
                        CASE WHEN ip.price_1m IS NOT NULL THEN cp.current_price - ip.price_1m ELSE NULL END,
                        CASE WHEN ip.price_1m IS NOT NULL AND ip.price_1m > 0 
                            THEN ((cp.current_price - ip.price_1m) / ip.price_1m) * 100 ELSE NULL END,
                            
                        ip.price_3m,
                        CASE WHEN ip.price_3m IS NOT NULL THEN cp.current_price - ip.price_3m ELSE NULL END,
                        CASE WHEN ip.price_3m IS NOT NULL AND ip.price_3m > 0 
                            THEN ((cp.current_price - ip.price_3m) / ip.price_3m) * 100 ELSE NULL END,
                            
                        ip.price_6m,
                        CASE WHEN ip.price_6m IS NOT NULL THEN cp.current_price - ip.price_6m ELSE NULL END,
                        CASE WHEN ip.price_6m IS NOT NULL AND ip.price_6m > 0 
                            THEN ((cp.current_price - ip.price_6m) / ip.price_6m) * 100 ELSE NULL END,
                            
                        ip.price_1y,
                        CASE WHEN ip.price_1y IS NOT NULL THEN cp.current_price - ip.price_1y ELSE NULL END,
                        CASE WHEN ip.price_1y IS NOT NULL AND ip.price_1y > 0 
                            THEN ((cp.current_price - ip.price_1y) / ip.price_1y) * 100 ELSE NULL END
                            
                    FROM current_prices cp
                    LEFT JOIN previous_day_prices pdp ON cp.cardid = pdp.cardid AND cp.variant = pdp.variant AND cp.grade = pdp.grade
                    LEFT JOIN interval_prices ip ON cp.cardid = ip.cardid AND cp.variant = ip.variant AND cp.grade = ip.grade
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