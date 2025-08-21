const pgp = require('pg-promise')(); //User Postgres Helpers

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface CollectSealedAnalysisResponse {
    state: boolean,
    errors: number,
    log: number
}

// Initialize the process for sealed products
async function initializeSealedPriceChangesProcess(db: any): Promise<boolean> {
  try {
      // 1. Drop the staging table if it exists
      await db.none(`DROP TABLE IF EXISTS pfanalysis_staging_sealed_price_changes_daily`);
      
      // 2. Create staging table by copying structure from the main table
      await db.none(`
          CREATE TABLE pfanalysis_staging_sealed_price_changes_daily AS
          SELECT * FROM pfanalysis_sealed_price_changes_daily
          WHERE 1=0
      `);
      
      // 3. Add new sets to tracking if they don't exist
      await db.none(`
          INSERT INTO pfanalysis_tracking_sealed_price_changes_daily (setid, status)
          SELECT ps.setid, 'PENDING'
          FROM pfdata_sets ps
          WHERE NOT EXISTS (
              SELECT 1 FROM pfanalysis_tracking_sealed_price_changes_daily pt
              WHERE pt.setid = ps.setid
          )
      `);
      
      // 4. Reset status for all sets to pending
      await db.none(`
          UPDATE pfanalysis_tracking_sealed_price_changes_daily
          SET status = 'PENDING', 
              processed_at = NULL,
              record_count = 0
      `);
      
      return true;
  } catch (error) {
      console.error("Error initializing sealed price changes process:", error);
      return false;
  }
}

// Process a single set for sealed products
async function processSealedPriceChangesForSet(db: any, setid: string, logger: any): Promise<number> {
    try {
        // Update this set's status to IN_PROGRESS
        await db.none(`
            UPDATE pfanalysis_tracking_sealed_price_changes_daily
            SET status = 'IN_PROGRESS', processed_at = NOW()
            WHERE setid = $1
        `, [setid]);
        
        logger.info(`Processing sealed price changes for set: ${setid}`);
        
        // Process this set and insert into staging
        const result = await db.one(`
            WITH latest_date AS (
                SELECT updatedsource::date as date
                FROM pf_sealed_price_history
                WHERE updatedsource IS NOT NULL
                ORDER BY updatedsource DESC
                LIMIT 1
            ),
            current_prices AS (
                SELECT t.sealedid, t.price, t.updatedsource
                FROM pf_sealed_price_history t
                JOIN latest_date ld ON t.updatedsource::date = ld.date
                LEFT JOIN pf_sealed ps ON t.sealedid = ps.sealedid
                WHERE ps.setid = $1
            ),
            interval_prices AS (
                SELECT DISTINCT ON (t.sealedid, interval_type)
                    t.sealedid,
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
                LEFT JOIN pf_sealed_price_history ph
                    ON t.sealedid = ph.sealedid
                    AND ph.updatedsource::date <= t.updatedsource::date - interval_length
                    AND ph.updatedsource::date > t.updatedsource::date - (interval_length + INTERVAL '7 days')
                ORDER BY t.sealedid, interval_type, ph.updatedsource::date DESC
            ),
            inserted AS (
                INSERT INTO pfanalysis_staging_sealed_price_changes_daily
                SELECT 
                    -- Existing columns
                    t.sealedid,
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
                FROM current_prices t
                LEFT JOIN pf_sealed_price_history p1d  -- 1 day
                    ON t.sealedid = p1d.sealedid 
                    AND p1d.updatedsource::date = t.updatedsource::date - INTERVAL '1 day'
                LEFT JOIN interval_prices p1w
                    ON t.sealedid = p1w.sealedid 
                    AND p1w.interval_type = '1w'
                LEFT JOIN interval_prices p1m
                    ON t.sealedid = p1m.sealedid 
                    AND p1m.interval_type = '1m'
                LEFT JOIN interval_prices p3m
                    ON t.sealedid = p3m.sealedid 
                    AND p3m.interval_type = '3m'
                LEFT JOIN interval_prices p6m
                    ON t.sealedid = p6m.sealedid 
                    AND p6m.interval_type = '6m'
                LEFT JOIN interval_prices p1y
                    ON t.sealedid = p1y.sealedid 
                    AND p1y.interval_type = '1y'
                RETURNING 1
            )
            SELECT COUNT(*) as count FROM inserted
        `, [setid]);
        
        // Update status to COMPLETED with record count
        await db.none(`
            UPDATE pfanalysis_tracking_sealed_price_changes_daily
            SET status = 'COMPLETED', 
                processed_at = NOW(),
                record_count = $1
            WHERE setid = $2
        `, [result.count, setid]);
        
        logger.info(`Completed set ${setid} with ${result.count} sealed products`);
        return result.count;
    } catch (error) {
        // Update status to FAILED
        await db.none(`
            UPDATE pfanalysis_tracking_sealed_price_changes_daily
            SET status = 'FAILED', 
                processed_at = NOW(),
                error_message = $1
            WHERE setid = $2
        `, [error.message, setid]);
        
        logger.error(`Failed processing set ${setid}: ${error.message}`);
        throw error;
    }
}

// Check if all sets are completed
async function areAllSealedSetsProcessed(db: any): Promise<boolean> {
    const result = await db.one(`
        SELECT 
            (SELECT COUNT(*) FROM pfanalysis_tracking_sealed_price_changes_daily) as total,
            (SELECT COUNT(*) FROM pfanalysis_tracking_sealed_price_changes_daily WHERE status = 'COMPLETED') as completed
    `);
    
    return result.total === result.completed;
}

// Finalize the update
async function finalizeSealedPriceChangesUpdate(db: any, logger: any): Promise<boolean> {
    try {
        // Check if all sets are processed
        const allProcessed = await areAllSealedSetsProcessed(db);
        if (!allProcessed) {
            logger.warning("Not all sets are processed. Cannot finalize sealed update.");
            return false;
        }
        
        logger.info("All sets processed. Finalizing sealed update to main table.");
        
        // Use a transaction for the final update
        await db.tx(async (t: any) => {
            await t.none(`TRUNCATE TABLE pfanalysis_sealed_price_changes_daily`);
            await t.none(`
                INSERT INTO pfanalysis_sealed_price_changes_daily
                SELECT * FROM pfanalysis_staging_sealed_price_changes_daily
            `);
        });
        
        logger.info("Successfully updated main sealed table.");
        return true;
    } catch (error) {
        logger.error(`Failed to finalize sealed update: ${error.message}`);
        return false;
    }
}

// Clean up after successful completion
async function cleanupSealedPriceChangesProcess(db: any, logger: any): Promise<boolean> {
    try {
        logger.info("Cleaning up sealed staging and tracking tables");
        
        // Clean up the staging table
        await db.none(`TRUNCATE TABLE pfanalysis_staging_sealed_price_changes_daily`);
        
        // Reset the tracking table statuses but keep the entries for future runs
        await db.none(`
            UPDATE pfanalysis_tracking_sealed_price_changes_daily
            SET status = 'COMPLETED',
                processed_at = NOW()
        `);
        
        logger.info("Sealed cleanup completed");
        return true;
    } catch (error) {
        logger.error(`Failed to clean up sealed: ${error.message}`);
        return false;
    }
}

// Main function to coordinate the sealed process
export default async function CollectSealedAnalysis(db: any, method: string): Promise<CollectSealedAnalysisResponse> {
    let state = false;
    let errors = 0;
    
    // Initialize logging
    let logid = await logToDBStart(db, "collectSealedAnalysis", method);
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectSealedAnalysis' }),
            timestamp(),
            json()
        ),
        transports: [
            new transports.File({ filename: `logs/collectSealedAnalysis/${logid}.log` }),
        ],
    });
    
    logger.info("--- STARTING SEALED PRICE CHANGES ANALYSIS ---");
    
    try {
        // Initialize the process
        logger.info("Initializing sealed price changes process");
        await initializeSealedPriceChangesProcess(db);
        
        // Get pending sets
        logger.info("Getting list of sets to process for sealed products");
        const pendingSets = await db.any(`
            SELECT setid FROM pfanalysis_tracking_sealed_price_changes_daily 
            WHERE status = 'PENDING'
            ORDER BY setid
        `);
        
        logger.info(`Found ${pendingSets.length} sets to process for sealed products`);
        
        // Process each set
        for (const set of pendingSets) {
            try {
                await processSealedPriceChangesForSet(db, set.setid, logger);
            } catch (error) {
                errors++;
                logger.error(`Error processing sealed set ${set.setid}: ${error.message}`);
                // Continue with other sets even if one fails
            }
        }
        
        // Check if all sets processed successfully
        const allProcessed = await areAllSealedSetsProcessed(db);
        
        // Finalize if all sets processed
        if (allProcessed) {
            logger.info("All sealed sets processed successfully. Finalizing update.");
            const finalized = await finalizeSealedPriceChangesUpdate(db, logger);
            state = finalized;
            
            // If finalized successfully, clean up
            if (finalized) {
                await cleanupSealedPriceChangesProcess(db, logger);
            }
        } else {
            // Report sets that failed
            const failedSets = await db.any(`
                SELECT setid FROM pfanalysis_tracking_sealed_price_changes_daily
                WHERE status = 'FAILED'
                ORDER BY setid
            `);
            
            logger.warning(`${failedSets.length} sealed sets failed processing: ${failedSets.map(s => s.setid).join(', ')}`);
            logger.warning("Not finalizing sealed update due to failed sets");
            state = false;
        }
    } catch (error) {
        errors++;
        logger.error(`Unexpected error in CollectSealedAnalysis: ${error.message}`);
        state = false;
    }
    
    await logToDBEnd(db, logid, state ? "COMPLETED" : "FAILED", errors, `/logs/collectSealedAnalysis/${logid}.log`);
    logger.info("--- SEALED PRICE CHANGES ANALYSIS COMPLETED ---");
    
    return {
        state: state,
        errors: errors,
        log: logid
    };
}