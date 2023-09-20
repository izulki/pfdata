const schedule = require('node-schedule');

import CollectPrice from "./collect/collectPrice";
import CollectAnalysis from "./collect/collectAnalysis";
import CollectAllPortfolioValues from "./collect/collectAllPortfolioValues";



import DBConfig from './utils/db_config';
const pgp = require('pg-promise')();
const db = pgp(DBConfig());

//Logging
import { logToDBStart } from "./utils/logger";
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
      new transports.File({ filename: 'logs/main.log'}),
      new transports.Console()
    ],
});


async function main() {
    logger.info(`Main function started`)

    /** RUN COLLECTION EVERYDAY AT 1AM **/
    const job = schedule.scheduleJob('0 10 * * *', async function(fireDate){
        logger.info(` --- SYSTEM PRICE COLLECTION STARTED ---`);
        logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
        let collectedPrice = await CollectPrice(db, "", "SYSTEM")
        logger.info(collectedPrice);
        logger.info(` --- SYSTEM PRICE COLLECTION COMPLETED ---`);

        logger.info(` --- SYSTEM ANALYSIS COLLECTION STARTED ---`);
        logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
        let collectedAnalysis = await CollectAnalysis(db, "SYSTEM")
        logger.info(collectedAnalysis);
        logger.info(` --- SYSTEM ANALYSIS COLLECTION COMPLETED ---`);

        logger.info(` --- SYSTEM PORTFOLIO SNAPSHOT STARTED ---`);
        logger.info('This job was supposed to run at ' + fireDate + ', but actually ran at ' + new Date());
        let collectedSnapshot = await CollectAllPortfolioValues(db, "SYSTEM")
        logger.info(collectedSnapshot);
        logger.info(` --- SYSTEM PORTOFLIO SNAPSHOT COMPLETED ---`);
      });
      var list = schedule.scheduledJobs;
      console.log("List of jobs", list)
}
main();

