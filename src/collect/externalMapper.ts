
import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
const pgp = require('pg-promise')(); //User Postgres Helpers


const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance
//We want to get the prices in PriceCharting, which has a TCG Player ID
//All our cards come from pokemontcg.io, but it doesn't have any direct mapping to TCG Player ID
//Instead, it has a redirect to the page, which the ID can be inferred in the url

//We want to Map the CardIDs to a TCGPlayerID as well as a PriceChartingID

interface ExternalMapperResponse {
    state: boolean,
    errors: number,
    log: number
}

export default async function ExteralMapper(db: any, set: string): Promise<ExternalMapperResponse> {
    let errors = 0;

    async function getFinalUrl(initialUrl: string) {
        try {
            const response = await axios.get(initialUrl, {
                maxRedirects: 0, // Avoid following redirects automatically
                validateStatus: function (status) {
                    return status >= 300 && status < 400; // Capture 3xx redirect responses
                },
            });
            const redirectUrl = response.headers.location;
            // console.log('Redirect to:', redirectUrl);
            return redirectUrl;
        } catch (error) {
            console.error('Error occurred:', error.response ? error.response.status : error.message);
        }
    }

    async function extractIdentifier(url: any) {
        const regex = /\/product\/(\d+)/;
        const match = url.match(regex);
        return match ? match[1] : null;
    }




    if (set == "") {


    } else if (set == "all") {
        let sets = await db.any("SELECT setid, id FROM pfdata_sets ORDER BY id DESC", [true]);
        for (let i = 0; i < sets.length; i++) {
            let cardsInsertArray = [];
            let cardImageArray = [];

            //Calcualte amount of pages to iterate
            let pages = 1;
            try {
                let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`); //Get all cards for current setid
                pages = Math.ceil(setData.data.totalCount / 250);
            } catch (err) {
                // logger.error(`Error Fetching https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                // logger.error(err);
                errors++;
            }

            /** Go through each page of the results to prepare insert Arrays **/
            for (let j = 1; j <= pages; j++) {
                // logger.info(`FETCH https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`)
                let cards = [];
                try {
                    let response = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    cards = response.data.data
                } catch (err) {
                    // logger.error(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    errors++;
                }

                cards.forEach(async card => { //For Each Card, find the redirect URL to Extract the ID
                    try {
                        if (card?.tcgplayer?.url) {
                            let url = await getFinalUrl(card.tcgplayer.url);
                            let tcgid = await extractIdentifier(url)
                            console.log("cardid: ", card.id, " tcgid: ", tcgid)
                            console.log("Trying: ", `https://www.pricecharting.com/api/product?t=cfb2f0281758817fd7c836fa0acdd044fe6ebfcf&q=${card.name} ${card.number}`)
                        }
                    } catch (e) {
                    }
                })


            }




        }



    } else if (set != "" && set != "all") {
        let insertArray = []

        //Calcualte amount of pages to iterate
        let pages = 1;
        try {
            let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${set}`); //Get all cards for current setid
            pages = Math.ceil(setData.data.totalCount / 250);
        } catch (err) {
            // logger.error(`Error Fetching https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
            // logger.error(err);
            errors++;
        }

        /** Go through each page of the results to prepare insert Arrays **/
        for (let j = 1; j <= pages; j++) {
            // logger.info(`FETCH https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`)
            let cards = [];
            try {
                let response = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${set}&page=${j}`);
                cards = response.data.data
            } catch (err) {
                // logger.error(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                errors++;
            }

            for (let z=0; z<cards.length; z++) {
                try {
                    if (cards[z]?.tcgplayer?.url) {
                        let url = await getFinalUrl(cards[z].tcgplayer.url);
                        let tcgid = await extractIdentifier(url)

                        insertArray.push({
                            cardid: cards[z].id,
                            tcgid: tcgid
                        })

                        // console.log("cardid: ", card.id, " tcgid: ", tcgid)
                    }
                } catch (e) {
                }
            }
            


        }
        /** Insert Data into CardID <-> TCGID Table  **/
        const mappingCs = new pgp.helpers.ColumnSet([
            "cardid", "tcgid"
        ], {table: 'pf_map_cardid_tcgid'});
        const mappingOnConflict = ' ON CONFLICT(cardid, tcgid) DO NOTHING';
        let cardVariantsQuery = pgp.helpers.insert(insertArray, mappingCs) + mappingOnConflict;
        let insertMapping = [];
        try {
            insertMapping = await db.any(cardVariantsQuery);
            
        } catch (err) {
            // logger.error(`Error inserting variants`)
            errors++;
        }
    }



    return {
        state: true,
        errors: errors,
        log: 0
    }
}