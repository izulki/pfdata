const schedule = require('node-schedule');

import CollectPrice from "./collect/collectPrice";
import CollectAnalysis from "./collect/collectAnalysis";
import CollectAllPortfolioValues from "./collect/collectAllPortfolioValues";
import WarmUp from "./collect/warmUp"; // Import the WarmUp function

import DBConfig from './utils/db_config';
const pgp = require('pg-promise')();
const db = pgp(DBConfig());

//Logging
import { logToDBStart } from "./utils/logger";
import CollectCurrencyRates from "./collect/collectCurrencyRates";
import { collectCardPrices } from "./collect/collectCardPrices";
import { runDiscordCleanup } from "./maintain/discordCleanup";
import { runStripeReconcile } from "./maintain/stripeReconcile";
import { runStripeReconcileTEST } from "./maintain/stripeReconcileTEST";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

const logger = createLogger({
  levels: config.syslog.levels,
  format: combine(
    label({ label: 'Main' }),
    timestamp(),
    json()
  ),
  transports: [
    new transports.File({ filename: 'logs/main.log' }),
    new transports.Console()
  ],
});

async function runWarmUp() {
  try {
    logger.info(` --- PAGE WARM-UP STARTED ---`);
    const warmUpResults = await WarmUp(db);
    logger.info(` --- PAGE WARM-UP RESULTS ---`);
    logger.info(warmUpResults);
    logger.info(` --- PAGE WARM-UP COMPLETED ---`);
  } catch (e) {
    logger.error(` --- PAGE WARM-UP ERROR ---`);
    logger.error(e);
  }
}

async function main() {
  logger.info(`Main function started`);


  // RUN EVERY 5 MINUTES
  const discordCleanupJob = schedule.scheduleJob('*/5 * * * *', async function(fireDate) {
    logger.info(`Discord cleanup job was supposed to run at ${fireDate}, but actually ran at ${new Date()}`);
    await runDiscordCleanup(db, "SYSTEM");
  });

    // RUN EVERY 5 MINUTES
    const stripeReconcile = schedule.scheduleJob('*/5 * * * *', async function(fireDate) {
      logger.info(`Discord cleanup job was supposed to run at ${fireDate}, but actually ran at ${new Date()}`);
      await runStripeReconcile(db, "SYSTEM");
      await runStripeReconcileTEST(db, "SYSTEM");
    });

  // RUN EVERY 6 HOURS
  const rule = new schedule.RecurrenceRule();
  rule.hour = new schedule.Range(0, 23, 6);  // Every 6 hours
  rule.minute = 1;
  const warmUpJob = schedule.scheduleJob(rule, async function(fireDate) {
    logger.info(`Warm-up job was supposed to run at ${fireDate}, but actually ran at ${new Date()}`);
    await runWarmUp();
  });

  /** RUN COLLECTION EVERYDAY AT 1AM **/
  const dailyJob = schedule.scheduleJob('1 0 * * *', async function(fireDate) {
    try {
      logger.info(` --- SYSTEM CURRENCY RATE COLLECTION STARTED ---`);
      logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
      let collectedPrice = await CollectCurrencyRates(db, "SYSTEM")
      logger.info(collectedPrice);
      logger.info(` --- SYSTEM CURRENCY RATE COLLECTION COMPLETED ---`);
    } catch (e) {
      logger.info(` --- SYSTEM CURRENCY RATE COLLECTION ERROR ---`);
      logger.info(e);
    }

    try {
      logger.info(` --- NEW SYSTEM PRICE COLLECTION STARTED ---`);
      logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
      await collectCardPrices(db)
      logger.info(` --- NEW SYSTEM PRICE COLLECTION COMPLETED ---`);
    } catch (e) {
      logger.info(` --- NEW SYSTEM PRICE COLLECTION ERROR ---`);
      logger.info(e);
    }

    try {
      logger.info(` --- SYSTEM PRICE COLLECTION STARTED ---`);
      logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
      let collectedPrice = await CollectPrice(db, "", "SYSTEM")
      logger.info(collectedPrice);
      logger.info(` --- SYSTEM PRICE COLLECTION COMPLETED ---`);
    } catch (e) {
      logger.info(` --- SYSTEM PRICE COLLECTION ERROR ---`);
      logger.info(e);
    }

    try {
      logger.info(` --- SYSTEM ANALYSIS COLLECTION STARTED ---`);
      logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
      let collectedAnalysis = await CollectAnalysis(db, "SYSTEM")
      logger.info(collectedAnalysis);
      logger.info(` --- SYSTEM ANALYSIS COLLECTION COMPLETED ---`);
    } catch (e) {
      logger.info(` --- SYSTEM ANALYSIS COLLECTION ERROR ---`);
      logger.info(e);
    }

    try {
      logger.info(` --- SYSTEM PORTFOLIO SNAPSHOT STARTED ---`);
      logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
      let collectedSnapshot = await CollectAllPortfolioValues(db, "SYSTEM")
      logger.info(collectedSnapshot);
      logger.info(` --- SYSTEM PORTOFLIO SNAPSHOT COMPLETED ---`);
    } catch (e) {
      logger.info(` --- SYSTEM PORTOFLIO SNAPSHOT ERROR ---`);
      logger.info(e);
    }
  });

  // Log that both jobs are scheduled
  logger.info('Scheduled jobs:');
  logger.info('- Daily collection job: 1:00 AM');
  logger.info('- Warm-up job: Every 59 minutes');
}

main();