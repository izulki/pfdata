var argv = require('minimist')(process.argv.slice(2));
import CollectSets from "./collect/collectSets";
import CollectPrice from "./collect/collectPrice";
import CollectCards from "./collect/collectCards";
import CollectAnalysis from "./collect/collectAnalysis";

/** --- START OF LOGGING SETUP --- **/
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

const logger = createLogger({
    levels: config.syslog.levels,
    format: combine(
        label({ label: 'cli' }),
        timestamp(),
        json()
      ),
    transports: [
      new transports.Console(),
      new transports.File({ filename: 'logs/cli.log'}),
    ],
});
/** --- END OF LOGGING SETUP --- **/

/** --- START OF DB SETUP --- **/
import DBConfig from './utils/db_config';

const pgp = require('pg-promise')();
const db = pgp(DBConfig());
/** --- END OF DB SETUP --- **/

async function cli() {
    logger.info(` --- CLI called  with args: ${JSON.stringify(argv)} --- `)
    const collect = argv.collect;
    let meta: boolean;
    let image: boolean;
    let state = {};

    try {
        meta = argv.meta;
        image = argv.image;
        switch (collect) {
            case 'collectSets':
                logger.info(`Starting collectSets function meta: ${meta}, image: ${image}`)
                state = await CollectSets(db, meta, image, "MANUAL")
                logger.info(`Finished collectSets results: ${JSON.stringify(state)}`)
                break;
            case 'collectPrice':
                logger.info(`Starting collectPrice function`)
                state = await CollectPrice(db, "", "MANUAL")
                logger.info(`Finished collectPrice results: ${JSON.stringify(state)}`)
                break;
            case 'collectCards':
                logger.info(`Starting collectCards function`)
                state = await CollectCards(db, meta, image, "", "MANUAL")
                logger.info(`Finished collectCards results: ${JSON.stringify(state)}`)
                break;
            case 'collectAnalysis':
                logger.info(`Starting CollectAnalysis function`)
                state = await CollectAnalysis(db, "MANUAL");
                logger.info(`Finished CollectAnalysis results: ${JSON.stringify(state)}`)
                break;
            default:
                console.log('No such command')
        }
    } catch (err) {
        console.log(err)
    }

    logger.info(` --- CLI call complete --- `)

}




cli()