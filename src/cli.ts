var argv = require('minimist')(process.argv.slice(2));
import CollectSets from "./collect/collectSets";
import CollectPrice from "./collect/collectPrice";
import CollectCards from "./collect/collectCards";
import CollectAnalysis from "./collect/collectAnalysis";
import CollectSprites from "./collect/collectSprites";
import CollectAllPortfolioValues from "./collect/collectAllPortfolioValues";
import CollectSimilarityVectors from "./collect/collectSimilarityVectors";


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
        new transports.File({ filename: 'logs/cli.log' }),
    ],
});
/** --- END OF LOGGING SETUP --- **/

/** --- START OF DB SETUP --- **/
import DBConfig from './utils/db_config';
import ExteralMapper from "./collect/externalMapper";
import CollectCurrencyRates from "./collect/collectCurrencyRates";
import InitializePriceMapTable from "./collect/initializePriceMapTable";
import { collectCardPrices } from "./collect/collectCardPrices";
import WarmUp from "./collect/warmUp";
import { runDiscordCleanup } from "./maintain/discordCleanup";
import { runStripeReconcile } from "./maintain/stripeReconcile";
import { runStripeReconcileTEST } from "./maintain/stripeReconcileTEST";
import { collectSealedPrices } from "./collect/collectSealedPrices";
import CollectSealedAnalysis from "./collect/collectSealedAnalysis";
import collectSealedImages from "./collect/collectSealedImages";
import CollectGradedAnalysis from "./collect/collectGradedAnalysis";
import massCardEntry from "./collect/massCardEntry";
import CollectCardsJSON from "./collect/collectCardsJSON";

const pgp = require('pg-promise')();
const db = pgp(DBConfig());
/** --- END OF DB SETUP --- **/

async function cli() {
    logger.info(` --- CLI called  with args: ${JSON.stringify(argv)} --- `)
    const collect = argv.collect;
    let meta: boolean;
    let image: boolean;
    let state = {};
    let set = argv.set;

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
            case 'collectCardsJSON':
                logger.info(`Starting CollectCardsJSON function`)
                state = await CollectCardsJSON(db, meta, image, "me1", "MANUAL", "massentry/cards_json/me1.json")
                logger.info(`Finished collectCards results: ${JSON.stringify(state)}`)
                break;
            case 'collectAnalysis':
                logger.info(`Starting CollectAnalysis function`)
                state = await CollectAnalysis(db, "MANUAL");
                logger.info(`Finished CollectAnalysis results: ${JSON.stringify(state)}`)
                break;
            case 'collectGradedAnalysis':
                logger.info(`Starting collectGradedAnalysis function`)
                state = await CollectGradedAnalysis(db, "MANUAL");
                logger.info(`Finished collectGradedAnalysis results: ${JSON.stringify(state)}`)
                break;
            case 'collectSealedAnalysis':
                logger.info(`Starting collectSealedAnalysis function`)
                state = await CollectSealedAnalysis(db, "MANUAL");
                logger.info(`Finished collectSealedAnalysis results: ${JSON.stringify(state)}`)
                break;
            case 'collectSprites':
                await CollectSprites();
                break;
            case 'collectSealedImages':
                await collectSealedImages(db);
                break;
            case 'collectSimilarityVectors':
                await CollectSimilarityVectors(db, "MANUAL");
                break;
            case 'collectAllPortfolioValues':
                await CollectAllPortfolioValues(db, "MANUAL");
                break
            case 'ExternalMapper':
                await ExteralMapper(db, set);
                break
            case 'collectCurrencyRates':
                await CollectCurrencyRates(db, "MANUAL");
                break
            case 'initPriceMap':
                await InitializePriceMapTable(db, "MANUAL");
                break
            case 'collectCardPrices':
                await collectCardPrices(db);
                break
            case 'collectSealedPrices':
                await collectSealedPrices(db);
                break
            case 'warmUp':
                await WarmUp(db);
                break
            case 'discordCleanup':
                await runDiscordCleanup(db, "MANUAL");
                break
            case 'stripeReconcile':
                await runStripeReconcile(db, "MANUAL");
                break
            case 'stripeReconcileTEST':
                await runStripeReconcileTEST(db, "MANUAL");
                break
            case 'massCardEntry':
                await massCardEntry();
                break
            default:
                console.log('No such command')
        }
    } catch (err) {
        console.log(err)
    }

    logger.info(` --- CLI call complete --- `)

}




cli()