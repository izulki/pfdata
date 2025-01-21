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
}

async function getProductId(initialUrl: string): Promise<string> {
    try {
        const response = await axios.get(initialUrl, {
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });
        
        // Check location header in successful response
        if (response.headers?.location) {
            const redirectUrl = response.headers.location;
            const match = redirectUrl.match(/\/product\/(\d+)/);
            return match ? match[1] : '';
        }

        // Also check if there's a response.data.tcgplayer.url that might contain the ID
        if (response.data?.url) {
            const match = response.data.url.match(/\/product\/(\d+)/);
            return match ? match[1] : '';
        }

        throw new Error('No redirect URL found in response');
    } catch (error: any) {
        // Check for redirect in error response
        if (error.response?.headers?.location) {
            const redirectUrl = error.response.headers.location;
            const match = redirectUrl.match(/\/product\/(\d+)/);
            return match ? match[1] : '';
        }
        
        // If no redirect found in error, throw the original error
        console.error('Error fetching product ID:', error.message);
        throw error;
    }
}

//Axios Instance
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance

export default async function InitializePriceMapTable(db: any, method: string): Promise<collectPriceResponse> {
    let state = false;
    let errors = 0;

    const specificSets = [
        'sv8pt5'
    ];

    console.log("Starting Init Price Map for specific sets")
    
    // Get only the specified sets
    const setsQuery = `
        SELECT setid, id 
        FROM pfdata_sets 
        WHERE setid = ANY($1)
        ORDER BY id DESC
    `;
    
    let sets = await db.any(setsQuery, [specificSets]);
    console.log(`Found ${sets.length} sets to process`);

    for (let i = 0; i < sets.length; i++) {
        let pages = 1;
        try {
            console.log(`Processing Set: ${sets[i].setid}`)
            let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
            pages = Math.ceil(setData.data.totalCount/250);       
        } catch (err) {
            console.error(`Error fetching set data for ${sets[i].setid}:`, err);
            errors++;
            continue;
        }

        /** Go through each page of the results **/
        for (let j = 1; j <= pages; j++) {
            let cards = [];
            try {
                console.log(`Set ${sets[i].setid} - Page ${j}/${pages}`);
                let response = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                cards = response.data.data;
            } catch (err) {
                console.error(`Error fetching page ${j} for set ${sets[i].setid}:`, err);
                errors++;
                continue;
            }
        
            const cardPromises = cards.map(async (card) => {
                try {
                    // If we have TCGPlayer URL, try to get the product ID
                    if (card.tcgplayer?.url) {
                        try {
                            const pid = await getProductId(card.tcgplayer.url);
                            // If we have prices, map all variants
                            if (card.tcgplayer.prices && Object.keys(card.tcgplayer.prices).length > 0) {
                                return Object.keys(card.tcgplayer.prices).map(k => ({
                                    cardid: card.id,
                                    tcgp_id: pid,
                                    tcgp_variant: k,
                                    pf_variant: k,
                                }));
                            } else {
                                // No prices found, assume single variant
                                return [{
                                    cardid: card.id,
                                    tcgp_id: pid,
                                    tcgp_variant: 'normal',
                                    pf_variant: 'normal',
                                }];
                            }
                        } catch (e) {
                            console.error(`Failed to get product ID for card ${card.id}, defaulting to null:`, e.message);
                            // Failed to get product ID but we still want to record the card
                            return [{
                                cardid: card.id,
                                tcgp_id: null,
                                tcgp_variant: 'normal',
                                pf_variant: 'normal',
                            }];
                        }
                    } else {
                        // No TCGPlayer info at all, still record the card
                        console.log(`No TCGPlayer URL for card ${card.id}, recording with null values`);
                        return [{
                            cardid: card.id,
                            tcgp_id: null,
                            tcgp_variant: 'normal',
                            pf_variant: 'normal',
                        }];
                    }
                } catch (e) {
                    console.error(`Error processing card: ${card.id}`, e);
                    // Even in case of complete failure, record the card
                    return [{
                        cardid: card.id,
                        tcgp_id: null,
                        tcgp_variant: 'normal',
                        pf_variant: 'normal',
                    }];
                }
            });
        
            const cardMappings = (await Promise.all(cardPromises)).flat();
        
            if (cardMappings.length > 0) {
                const priceMapCs = new pgp.helpers.ColumnSet([
                    "cardid", 
                    { name: "tcgp_id", def: null }, // Add default null value
                    "tcgp_variant", 
                    "pf_variant"
                ], {table: 'pf_cards_pricing_map'});
                const priceMapOnConflict = ' ON CONFLICT DO NOTHING';
        
                let priceQuery = pgp.helpers.insert(cardMappings, priceMapCs) + priceMapOnConflict;
                try {
                    await db.any(priceQuery);
                    console.log(`Successfully inserted ${cardMappings.length} mappings for set ${sets[i].setid}`);
                } catch (err) {
                    console.error(`Error inserting prices for set ${sets[i].setid}:`, err);
                    errors++;
                }
            }
        }
    }

    let result: collectPriceResponse = {
        state: errors === 0,
        errors: errors,
    }

    return errors === 0 ? Promise.resolve(result) : Promise.reject(result);
}

/*
normal => Normal
holofoil => Holofoil
reverseHolofoil => Reverse Holofoil
1stEdition => 1st Edition
unlimited => Unlimited
unlimitedHolofoil => Unlimited Holofoil
1stEditionHolofoil => 1st Edition Holofoil  */