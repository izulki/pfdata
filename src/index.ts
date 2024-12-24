const schedule = require('node-schedule');

import CollectPrice from "./collect/collectPrice";
import CollectAnalysis from "./collect/collectAnalysis";
import CollectAllPortfolioValues from "./collect/collectAllPortfolioValues";



import DBConfig from './utils/db_config';
const pgp = require('pg-promise')();
const db = pgp(DBConfig());

//Logging
import { logToDBStart } from "./utils/logger";
import CollectCurrencyRates from "./collect/collectCurrencyRates";
import { collectCardPrices } from "./collect/collectCardPrices";
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


async function main() {
  logger.info(`Main function started`)
  /** RUN COLLECTION EVERYDAY AT 1AM **/
  const job = schedule.scheduleJob('0 10 * * *', async function (fireDate) {
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
}
main();

