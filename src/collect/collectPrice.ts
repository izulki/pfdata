import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

const pgp = require('pg-promise')(); //User Postgres Helpers


interface collectPriceResponse {
    state: boolean,
    errors: number,
    log: number
}

//Axios Instance
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance

export default async function CollectPrice(db: any, set: string, method: string): Promise<collectPriceResponse> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectPrice", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectPrice' }),
            timestamp(),
            json()
            )  
            ,
        transports: [
            new transports.File({ filename: `logs/collectPrice/${logid}.log`}),
        ],
    });

    logger.info(" --- STARTING --- ")
    if (set == "") { //Set identifier is empty, find price for all cards.
        logger.info("Updating price for ALL Cards.")

        /*** Get All Sets From Database ***/
        let sets = await db.any("SELECT setid, id FROM pfdata_sets ORDER BY id DESC", [true]);
        for (let i=0; i<sets.length; i++) {
            let cardPriceInsertArray = [];

            //Calcualte amount of pages to iterate
            let pages = 1;
            try {
                let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                pages = Math.ceil(setData.data.totalCount/250);       
            } catch (err) {
                logger.err(`Error Fetching https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                logger.err(err);
                errors++;
            }

            /** Go through each page of the results to prepare insert Arrays **/
            for (let j=1; j<=pages; j++) { 
                logger.info(`FETCH https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`)            
                let cards = [];
                try {
                    let response = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    cards = response.data.data
                } catch (err) {
                    logger.err(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    errors++;
                }


                logger.info(`Prepare Insert Array with Price Data`)
                cards.forEach(card => {
                    cardPriceInsertArray.push(
                        {
                            cardid: card.id,
                            source: "tcgplayer",
                            prices: "tcgplayer" in card ? card.tcgplayer.prices : null,
                            updatedsource: "tcgplayer" in card ? card.tcgplayer.updatedAt : null,
                        }
                    )
                })
            }


            /** Insert Data into Price Table  **/
            logger.info(`Inserting Prices into Database`)
            const priceCs = new pgp.helpers.ColumnSet([
                "cardid", "source", "prices", "updatedsource", {
                    name: 'updated',
                    def: () => new Date() // default to the current Date/Time
                }
            ], {table: 'pfdata_cardprices'});
            const pricesOnConflict = ' ON CONFLICT(cardid, source, updatedsource) DO NOTHING';

            let priceQuery = pgp.helpers.insert(cardPriceInsertArray, priceCs) + pricesOnConflict;
            let insertPrices = [];
            try {
                insertPrices = await db.any(priceQuery);
            } catch (err) {
                logger.error(`Error inserting prices into database`)
                errors++;
            }
        }

    } else { //Get Specific set

    }

    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectPrice/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: collectPriceResponse = {
        state: state,
        errors: errors,
        log: logid
    }

    if ( errors == 0) {
        result.state = true;
        return Promise.resolve(result)
    } else {
        result.state = false;
        return Promise.reject(result)
    }
}