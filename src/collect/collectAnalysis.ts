

const pgp = require('pg-promise')(); //User Postgres Helpers

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface collectAnalysisResponse {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectAnalysis(db: any, method: string): Promise<collectAnalysisResponse> {
    // --- CALCULATE CHANGE PAST 3 DAYS ---
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectAnalysis", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectAnalysis' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/collectAnalysis/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")

    //Get cards and its prices from today and three days ago.
    logger.info("Fetch Cards and Prices")
    let data = await db.any(query, [true]);
    let prices = [];
    logger.info("Fetch Cards and Prices Complete")

    logger.info("Extracting Prices and Structuring Data")
    for (let i=0; i<data.length; i++) {
        //For Each Variant - Find Prices
        if (data[i].nowprice != null && data[i].thenprice != null) {
            //Extract Now Price
            for (const [key, value] of Object.entries(data[i].nowprice)) {
                try {
                    let variant = key;
                    let nowprice = value['market'];
                    let thenprice = data[i].thenprice[`${variant}`]['market']
                    let change = (nowprice - thenprice) / thenprice;
                    
                    if (nowprice && thenprice) {
                        let obj = {
                            cardid: data[i].cardid,
                            card: data[i].card,
                            variant: variant,
                            set: data[i].set,
                            image: data[i].images.large ? data[i].images.large  : '',
                            nowprice: nowprice,
                            thenprice: thenprice,
                            change_3d: change
                        }
                        prices.push(obj)
                    }
                } catch (e) {
                    logger.info(`Null Error: ${data[i].cardid}`)
                }

            }
        }
    }
    logger.info("Extracting Prices and Structuring Data Complete")

    
    // ---- SORT --- 
    let sorted = await prices.sort((a,b) => b.change_3d - a.change_3d);


    logger.info("Clear analysis table and Insert Into DB")
    const analysisCs = new pgp.helpers.ColumnSet(['cardid', 'card', 'variant', 'set', 'image', 'nowprice', 'thenprice', 'change_3d'], {table: 'pfanalysis_change'})
    const analysisQuery = pgp.helpers.insert(sorted, analysisCs); 
    
    try {
        const del = await db.any('delete from public.pfanalysis_change')
        const insertAnalysis = await db.any(analysisQuery);
    } catch (e) {
        errors++;
    }
    logger.info("Clear analysis table and Insert Into DB Complete")

    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectAnalysis/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: collectAnalysisResponse = {
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

let query = `
select t1.cardid, t1.prices as nowprice, t2.prices as thenprice, 
c.name as card, ps.name as set, c.images
from (
    select t.cardid, cp.prices from pfdata_cardprices cp inner join 
    (
        select cardid, max(updated) as mupdated
        from pfdata_cardprices
        group by cardid
    ) t
    on cp.cardid = t.cardid 
    and cp.updated = t.mupdated
    where prices is not null
) t1
left join 
(
    select *, date_trunc('day', cp.updated) from pfdata_cardprices cp
    where date_trunc('day', now() - interval '3 days') = date_trunc('day', cp.updated)
    and prices is not null
) t2
on t1.cardid = t2.cardid
left join pfdata_cards c on t1.cardid = c.cardid 
left join pfdata_sets ps on c.setid = ps.setid`