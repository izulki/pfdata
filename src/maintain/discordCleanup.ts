import { logToDBEnd, logToDBStart } from "../utils/logger";

const axios = require('axios');
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

export async function runDiscordCleanup(
    db: any,
    method: string = "default"
) {
    let logid = await logToDBStart(db, "discordCleanup", method); //Send to start log to DB, return log id.
    let errors = 0;

    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'discordCleanup' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/discordCleanup/${logid}.log` }),
        ],
    });

    try {
      logger.info(` --- DISCORD MAINTENANCE STARTED ---`);
      
      const API_POKEFOLIO_BASE_URL = process.env.API_POKEFOLIO_BASE_URL;
      
      // Step 1: Discord Cleanup
      logger.info(` --- DISCORD CLEANUP STARTED ---`);
      try {
        const cleanupResponse = await axios.post(`${API_POKEFOLIO_BASE_URL}/admin/discord/cleanup`, {}, {
          timeout: 30000, // 30 second timeout
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': process.env.POKEFOLIO_ADMIN_API_KEY
          }
        });
    
        if (cleanupResponse.data.success) {
          logger.info(` --- DISCORD CLEANUP RESULTS ---`);
          logger.info({
            totalChecked: cleanupResponse.data.totalLinksChecked,
            removed: cleanupResponse.data.linksRemoved,
            processingTime: cleanupResponse.data.processingTimeMs,
            removedUsers: cleanupResponse.data.removedUsers.length > 0 ? cleanupResponse.data.removedUsers : 'None'
          });
          logger.info(` --- DISCORD CLEANUP COMPLETED ---`);
        } else {
          errors++
          logger.error(` --- DISCORD CLEANUP FAILED ---`);
          logger.error(cleanupResponse.data);
        }
      } catch (cleanupError) {
        logger.error(` --- DISCORD CLEANUP ERROR ---`);
        if (cleanupError.response) {
          errors++
          logger.error({
            status: cleanupError.response.status,
            data: cleanupError.response.data,
            url: cleanupError.config?.url
          });
        } else if (cleanupError.request) {
          errors++
          logger.error('No response received from Discord cleanup API');
          logger.error(cleanupError.message);
        } else {
          errors++
          logger.error(cleanupError.message);
        }
      }

      // Step 2: Discord Role Sync
      logger.info(` --- DISCORD ROLE SYNC STARTED ---`);
      try {
        const syncResponse = await axios.post(`${API_POKEFOLIO_BASE_URL}/admin/discord/sync-all`, {}, {
          timeout: 120000, // 2 minute timeout (longer for bulk operation)
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': process.env.POKEFOLIO_ADMIN_API_KEY
          }
        });
    
        if (syncResponse.data.success) {
          logger.info(` --- DISCORD ROLE SYNC RESULTS ---`);
          logger.info({
            totalUsers: syncResponse.data.totalUsers,
            processedUsers: syncResponse.data.processedUsers,
            skippedUsers: syncResponse.data.skippedUsers,
            totalChanges: syncResponse.data.totalChanges,
            totalRolesAdded: syncResponse.data.summary.totalRolesAdded,
            totalRolesRemoved: syncResponse.data.summary.totalRolesRemoved,
            errorCount: syncResponse.data.errors.length
          });
          
          // Log individual errors if any
          if (syncResponse.data.errors.length > 0) {
            logger.warn(` --- DISCORD ROLE SYNC ERRORS ---`);
            syncResponse.data.errors.forEach((error: any, index: number) => {
              logger.warn({
                errorIndex: index + 1,
                userId: error.userId,
                discordId: error.discordId,
                error: error.error
              });
            });
            // Count API-level errors but don't fail the entire operation
            errors += syncResponse.data.errors.length;
          }
          
          logger.info(` --- DISCORD ROLE SYNC COMPLETED ---`);
        } else {
          errors++
          logger.error(` --- DISCORD ROLE SYNC FAILED ---`);
          logger.error(syncResponse.data);
        }
      } catch (syncError) {
        logger.error(` --- DISCORD ROLE SYNC ERROR ---`);
        if (syncError.response) {
          errors++
          logger.error({
            status: syncError.response.status,
            data: syncError.response.data,
            url: syncError.config?.url
          });
        } else if (syncError.request) {
          errors++
          logger.error('No response received from Discord role sync API');
          logger.error(syncError.message);
        } else {
          errors++
          logger.error(syncError.message);
        }
      }

      logger.info(` --- DISCORD MAINTENANCE COMPLETED ---`);
      logger.info({
        totalErrors: errors,
        completedOperations: ['cleanup', 'roleSync']
      });

    } catch (error) {
      logger.error(` --- DISCORD MAINTENANCE CRITICAL ERROR ---`);
      errors++
      logger.error(error.message);
    } finally {
        logToDBEnd(db, logid, "COMPLETED", errors, `/logs/discordCleanup/${logid}.log`)
    }
}