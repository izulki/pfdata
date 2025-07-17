import { logToDBEnd, logToDBStart } from "../utils/logger";

const axios = require('axios');
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

export async function runStripeReconcileTEST(
    db: any,
    method: string = "default"
) {
    let logid = await logToDBStart(db, "stripeReconcile-TEST", method); //Send to start log to DB, return log id.
    let errors = 0;

    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'stripeReconcile-TEST' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/stripeReconcile-TEST/${logid}.log` }),
        ],
    });

    try {
      logger.info(` --- STRIPE MAINTENANCE STARTED ---`);
      
      const API_POKEFOLIO_BASE_URL = process.env.API_POKEFOLIO_BASE_TEST_URL;
      
      // Step 1: Stripe Cleanup
      logger.info(` --- STRIPE RECONCILE STARTED ---`);

      try {
        const reconcileResponse = await axios.post(`${API_POKEFOLIO_BASE_URL}/admin/stripe/reconcile-subscriptions`, {}, {
          timeout: 30000, // 30 second timeout
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': process.env.POKEFOLIO_ADMIN_API_KEY
          }
        });

       const result = await reconcileResponse?.data


        if (result.success) {
          logger.info(` --- STRIPE CLEANUP RESULTS ---`);
          logger.info(result);
          logger.info(` --- STRIPE CLEANUP COMPLETED ---`);
        } else {
          errors++
          logger.error(` --- STRIPE CLEANUP FAILED ---`);
          logger.error(result);
        }
      } catch (cleanupError) {
        logger.error(` --- STRIPE CLEANUP ERROR ---`);
        logger.error(cleanupError);
        if (cleanupError.response) {
          errors++
        } else if (cleanupError.request) {
          errors++
          logger.error('No response received from Stripe cleanup API');
          logger.error(cleanupError.message);
        } else {
          errors++
          logger.error(cleanupError.message);
        }
      }


      logger.info(` --- STRIPE MAINTENANCE COMPLETED ---`);
      logger.info({
        totalErrors: errors,
        completedOperations: ['cleanup', 'roleSync']
      });

    } catch (error) {
      logger.error(` --- STRIPE MAINTENANCE CRITICAL ERROR ---`);
      errors++
      logger.error(error.message);
    } finally {
        logToDBEnd(db, logid, "COMPLETED", errors, `/logs/stripeReconcile-TEST/${logid}.log`)
    }
}