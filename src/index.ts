import CollectCards from "./collect/collectCards";
import CollectSets from "./collect/collectSets";



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
    ],
});


async function main() {
    logger.info(`Main function started`)


    //Date
    const timeElapsed = Date.now();
    const today = new Date(timeElapsed);

    /** SET COLLECTION FLAGS*/
    let SetMetaFlag = true;
    let SetImagesFlag = false;

    // //Every Saturday (Day 6) Or First of Month, Collect Images
    // if (today.getDay() == 6 || today.getDate() == 1 ) SetImagesFlag = true;
    // console.log(`${today.toUTCString()}`," - ", ` STARTING SET COLLECTION: (${SetMetaFlag} ${SetImagesFlag})`)
    let collectedSets = await CollectSets(db, SetMetaFlag, SetImagesFlag, "SYSTEM");
    // console.log(`${today.toUTCString()}`," - ", ` COMPLETED SET COLLECTION: Result: ${collectedSets}`)

    // /** CARD COLLECTION FLAGS*/
    // let CardMetaFlag = true;
    // let CardImageFlag = false;

    // //Every Two Weeks, Collect Images
    // if (today.getDate() == 1 || today.getDate() == 14) CardImageFlag = true;
    // console.log(`${today.toUTCString()}`," - ", ` STARTING CARD COLLECTION: (${SetMetaFlag} ${CardImageFlag})`)
    // let collectedCards = await CollectCards(db, CardMetaFlag, CardImageFlag);
    // console.log(`${today.toUTCString()}`," - ", ` COMPLETED CARD COLLECTION: Result: ${collectedCards}`)

}

main();

