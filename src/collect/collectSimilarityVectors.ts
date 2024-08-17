const pgp = require('pg-promise')(); //User Postgres Helpers
const dayjs = require('dayjs');

const natural = require('natural');
const TfIdf = natural.TfIdf;


/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

interface CollectAllPortfolioValues {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectSimilarityVectors(db: any, method: string): Promise<CollectAllPortfolioValues> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    // let logid = await logToDBStart(db, "CollectSimilarityVectors", method); //Send to start log to DB, return log id.
    // const logger = createLogger({
    //     levels: config.syslog.levels,
    //     format: combine(
    //         label({ label: 'similarityVectors' }),
    //         timestamp(),
    //         json()
    //     )
    //     ,
    //     transports: [
    //         new transports.File({ filename: `logs/similarityVectors/${logid}.log` }),
    //     ],
    // });

    // logger.info(" --- STARTING --- ")

    // //Get cards and its prices from today and three days ago.
    // logger.info("Fetch Distinct Users who have Inventory")

    let selectSetsQuery = `
    select * from pfdata_sets
    `

    let sets = await db.any(selectSetsQuery, [true]);

    for (let k=0; k<sets.length; k++) {
        console.log("Vectorizing set: ", sets[k].setid)

        let selectCardsQuery = `
        select cardid, name, subtypes, rules, attacks, abilities, hp, number, artist from pfdata_cards pc where pc.setid = '${sets[k].setid}' 
        `
    let cards = await db.any(selectCardsQuery, [true]);

    let combined = []

    //For Each Card
    for (let i = 0; i < cards.length; i++) {
        let combine = {cardid: cards[i].cardid, vector: [], vector_short: []}


        if (cards[i].name) {
            combine.vector.push(cards[i].name) 
            combine.vector_short.push(cards[i].name)
        }


        if (cards[i].subtypes) combine.vector.push(...cards[i].subtypes)
        if (cards[i].rules) combine.vector.push(...cards[i].rules)
        if (cards[i].attacks) {
            cards[i].attacks.forEach((attack: any) => {
                let obj = JSON.parse(attack);
                combine.vector.push(obj.name)
                combine.vector.push(obj.damage)
                combine.vector.push(obj.text)
            })
        }
        if (cards[i].abilities) {
            cards[i].abilities.forEach((attack: any) => {
                let obj = JSON.parse(attack);
                combine.vector.push(obj.name)
                combine.vector.push(obj.text)
            })
        }
        if (cards[i].hp) {
            combine.vector.push(cards[i].hp)
            //combine.vector_short.push(cards[i].hp)
        }
        if (cards[i].number) {
            combine.vector.push(cards[i].number)
            combine.vector_short.push(cards[i].number)
        }
        if (cards[i].artist) {
            combine.vector.push(cards[i].artist)
            //combine.vector_short.push(cards[i].artist)
        }

        combined.push(combine)
    }

    combined.forEach(card => {
        db.any(
            'INSERT INTO pf_ml_vectors (cardid, vector, vector_short) VALUES ($1, $2, $3)',
            [card.cardid, card.vector, card.vector_short],
            (err, res) => {
              if (err) throw err;
            }
        );
    })

    }





    //const tfidf = new TfIdf();
    //combined.forEach(card => tfidf.addDocument(card));

    // combined.forEach(async (card, i) => {
    //     const vector = {};
    //     tfidf.listTerms(i).forEach(item => {
    //         vector[item.term] = item.tfidf;
    //       });
        
    //     const cardid = card.split(' ')[0];
    //     const fuzzy = card.split(' ')[1];


    //     await db.any(
    //         'INSERT INTO pf_ml_vectors (cardid, vector) VALUES ($1, $2)',
    //         [cardid, fuzzy],
    //         (err, res) => {
    //           if (err) throw err;
    //         }
    //     );

    //     await db.any(
    //         'INSERT INTO pf_ml_vectors (cardid, tfidf_vector) VALUES ($1, $2)',
    //         [cardid, vector],
    //         (err, res) => {
    //           if (err) throw err;
    //         }
    //       );
    // })

   


    // let portfolioSnapshots = [];

    // for (let i=0; i<users.length; i++) {
    //     let total = 0;
    //     let query = `
    //         select * from pf_inventory pi2 
    //         left join pfdata_cardprices pc on pc.cardid = pi2.cardid 
    //         where userid = '${users[i].userid}'
    //         AND pi2.status = true
    //         AND updated = (SELECT max(cp1.updated) FROM pfdata_cardprices cp1 where cp1.cardid = pc.cardid)
    //     `
    //     let cards = await db.any(query, [true]);
    //     for (let j=0; j<cards.length; j++) {
    //         if (cards[j].prices) {
    //             let price = 0;
    //             try {   
    //                 price = cards[j].prices[`${cards[j].variant}`]['market']
    //             } catch (e) {
    //             }
    //             total = total + price;
    //         }
    //     }

    //     portfolioSnapshots.push(
    //         {
    //             userid: users[i].userid,
    //             value: total,
    //             date: dayjs().format('YYYY/MM/DD')
    //         }
    //     )


    // }

    // //logger.info("Upload Portfolio Snapshots")

    // const portfolioSnapshotCs = new pgp.helpers.ColumnSet([
    //     "userid", "value", "date", {
    //         name: 'timestamp',
    //         def: () => new Date() // default to the current Date/Time
    //     }
    // ], {table: 'pf_portfoliosnapshots'});

    // const onConflict = ' ON CONFLICT(userid, date) DO NOTHING';

    // let insertQuery = pgp.helpers.insert(portfolioSnapshots, portfolioSnapshotCs) + onConflict;
    // let insertSnapshot = [];
    // try {
    //     insertSnapshot = await db.any(insertQuery);
    // } catch (err) {
    //     logger.error(`Error inserting prices into database`)
    //     errors++;
    // }

    // logToDBEnd(db, logid, "COMPLETED", errors, `/logs/AllPortfolioValues/${logid}.log`)
    // logger.info(" --- COMPLETED --- ")

    let result: CollectAllPortfolioValues = {
        state: state,
        errors: errors,
        log: 1000000
    }

    if (errors == 0) {
        result.state = true;
        return Promise.resolve(result)
    } else {
        result.state = false;
        return Promise.reject(result)
    }

}


