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
            let cardVariantsInsertArray = [];

            let setPrice = 0;
            let updatedSetSource = "";

            //Calcualte amount of pages to iterate
            let pages = 1;
            try {
                let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                pages = Math.ceil(setData.data.totalCount/250);       
            } catch (err) {
                logger.error(`Error Fetching https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                logger.error(err);
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
                    logger.error(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
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

                    /** --- SET PRICE DETERMINED BY ALL VARIATIONS OF TCGPLAYER MARKET VALUE --- */
                    if (card.tcgplayer?.prices) { //Add to set
                    for (const key in card.tcgplayer.prices) { //Loop through to find market value of each version
                        cardVariantsInsertArray.push({cardid: card.id, variant: key})//Save each variant
                        setPrice = setPrice + card.tcgplayer.prices[key].market;
                        updatedSetSource = card.tcgplayer.updatedAt;
                    }
                   }
                })
            }

            /** Insert Data into Variants Table  **/
            //cardVariantsInsertArray
            logger.info(`Insert Set Price into Database`)
            const cardVariantsCs = new pgp.helpers.ColumnSet([
                "cardid", "variant"
            ], {table: 'pfdata_variants'});
            const cardVariantsOnConflict = ' ON CONFLICT(cardid, variant) DO NOTHING';
            let cardVariantsQuery = pgp.helpers.insert(cardVariantsInsertArray, cardVariantsCs) + cardVariantsOnConflict;
            let insertVariants = [];

            try {
                insertVariants = await db.any(cardVariantsQuery);
            } catch (err) {
                logger.error(`Error inserting variants`)
                errors++;
            }

            /** Insert Data into Set Price Table  **/
            logger.info(`Insert Set Price into Database`)
            const setPriceCs = new pgp.helpers.ColumnSet([
                "setid", "price", "updatedsource", "source", {
                    name: 'updated',
                    def: () => new Date() // default to the current Date/Time
                }
            ], {table: 'pfdata_setprices'});
            const setPriceOnConflict = ' ON CONFLICT(setid, source, updatedsource) DO NOTHING';
            let setPriceQuery = pgp.helpers.insert([{setid: sets[i].setid, price: setPrice, updatedsource: updatedSetSource, source: "tcgplayer"}], setPriceCs) + setPriceOnConflict;
            let insertSetPrice = [];
            try {
                insertSetPrice = await db.any(setPriceQuery);
            } catch (err) {
                logger.error(`Error inserting set price into database`)
                errors++;
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