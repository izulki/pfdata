const pgp = require('pg-promise')(); //User Postgres Helpers
const dayjs = require('dayjs');

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface CollectAllPortfolioValues {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectAllPortfolioValues(db: any, method: string): Promise<CollectAllPortfolioValues> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectAllPortfolioValues", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'AllPortfolioValues' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/AllPortfolioValues/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")

    //Get cards and its prices from today and three days ago.
    logger.info("Fetch Distinct Users who have Inventory")
    let users = await db.any(distinctUsersQuery, [true]);

    logger.info("Go through each user and cards and find total, store in array")

    let portfolioSnapshots = [];

    for (let i=0; i<users.length; i++) {
        let total = 0;
        let query = `
            select * from pf_inventory pi2 
            left join pfdata_cardprices pc on pc.cardid = pi2.cardid 
            where userid = '${users[i].userid}'
            AND pi2.status = true
            AND updated = (SELECT max(cp1.updated) FROM pfdata_cardprices cp1 where cp1.cardid = pc.cardid)
        `
        let cards = await db.any(query, [true]);
        for (let j=0; j<cards.length; j++) {
            if (cards[j].prices) {
                let price = 0;
                try {   
                    price = cards[j].prices[`${cards[j].variant}`]['market']
                } catch (e) {
                }
                total = total + price;
            }
        }

        portfolioSnapshots.push(
            {
                userid: users[i].userid,
                value: total,
                date: dayjs().format('YYYY/MM/DD')
            }
        )

        
    }

    logger.info("Upload Portfolio Snapshots")
    console.log(portfolioSnapshots)

    const portfolioSnapshotCs = new pgp.helpers.ColumnSet([
        "userid", "value", "date", {
            name: 'timestamp',
            def: () => new Date() // default to the current Date/Time
        }
    ], {table: 'pf_portfoliosnapshots'});

    const onConflict = ' ON CONFLICT(userid, date) DO NOTHING';

    let insertQuery = pgp.helpers.insert(portfolioSnapshots, portfolioSnapshotCs) + onConflict;
    let insertSnapshot = [];
    try {
        insertSnapshot = await db.any(insertQuery);
    } catch (err) {
        logger.error(`Error inserting prices into database`)
        errors++;
    }

    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/AllPortfolioValues/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: CollectAllPortfolioValues = {
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

let distinctUsersQuery = `
    select distinct(userid) from pf_inventory pi2 
`