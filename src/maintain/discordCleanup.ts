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
      logger.info(` --- DISCORD CLEANUP STARTED ---`);
      
      const API_POKEFOLIO_BASE_URL = process.env.API_POKEFOLIO_BASE_URL; // Adjust to your API URL
      const response = await axios.post(`${API_POKEFOLIO_BASE_URL}/admin/discord/cleanup`, {}, {
        timeout: 30000, // 30 second timeout
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': process.env.POKEFOLIO_ADMIN_API_KEY
        }
      });
  
      if (response.data.success) {
        logger.info(` --- DISCORD CLEANUP RESULTS ---`);
        logger.info({
          totalChecked: response.data.totalLinksChecked,
          removed: response.data.linksRemoved,
          processingTime: response.data.processingTimeMs,
          removedUsers: response.data.removedUsers.length > 0 ? response.data.removedUsers : 'None'
        });
        logger.info(` --- DISCORD CLEANUP COMPLETED ---`);
      } else {
        errors++
        logger.error(` --- DISCORD CLEANUP FAILED ---`);
        logger.error(response.data);
      }
    } catch (error) {
      logger.error(` --- DISCORD CLEANUP ERROR ---`);
      if (error.response) {
        // API responded with error status
        errors++
        logger.error({
          status: error.response.status,
          data: error.response.data,
          url: error.config?.url
        });
      } else if (error.request) {
        // Request made but no response received
        errors++
        logger.error('No response received from Discord cleanup API');
        logger.error(error.message);
      } else {
        // Something else happened
        errors++
        logger.error(error.message);
      }
    } finally {
        logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectCards/${logid}.log`)
    }
  }