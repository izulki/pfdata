const pgp = require('pg-promise')();
import axios from 'axios';
import { getAuthToken } from '../utils/getAuthToken';
import dayjs from 'dayjs';
require('dotenv').config();

interface PriceRecord {
    cardid: string;
    tcgp_id: string;
    tcgp_variant: string;
    pf_variant: string;
    setid: string;
}

interface TCGPriceResult {
    productId: number;
    marketPrice: number | null;
    subTypeName: string;
}

interface ProductVariants {
    [productId: string]: {
        variants: string[];
        cards: PriceRecord[];
    }
}

export async function collectCardPrices(db: any): Promise<void> {
    try {
        const sets = await db.any("SELECT setid FROM pfdata_sets ORDER BY releaseddate DESC");
        const token = await getAuthToken();
        const totalSets = sets.length;
        const timeStarted = new Date();
        
        let logId: number;
        try {
            const result = await db.one(`
                INSERT INTO pf_logs_price_update (progress, time_started, progress_details, flagged_cards)
                VALUES (0, $1, '{}', '{}')
                RETURNING id
            `, [timeStarted]);
            logId = result.id;
        } catch (err) {
            console.error('Failed to create log entry:', err);
            throw err;
        }

        const progressDetails: { [key: string]: any } = {};
        const flaggedCardsCount: { [key: string]: number } = {};

        for (let setIndex = 0; setIndex < sets.length; setIndex++) {
            const set = sets[setIndex];
            const progress = ((setIndex + 1) / totalSets * 100).toFixed(1);
            //console.log(`\nProcessing set ${set.setid} (${setIndex + 1}/${totalSets} - ${progress}%)`);

            const query = `
                SELECT DISTINCT pcpm.*, pc.setid 
                FROM pf_cards_pricing_map pcpm
                LEFT JOIN pfdata_cards pc ON pc.cardid = pcpm.cardid
                WHERE pc.setid = $1
            `;
            const cards: PriceRecord[] = await db.any(query, [set.setid]);
            
            if (cards.length === 0) {
                //console.log(`No cards found for set ${set.setid}`);
                continue;
            }

            const uniqueCardVariants = new Set();
            const duplicates = new Set();
            const skippedCards = new Set();
            const nullPriceCards = new Set();
            const failedCards = new Set();
            
            cards.forEach(card => {
                const key = `${card.cardid}-${card.pf_variant}`;
                if (uniqueCardVariants.has(key)) {
                    duplicates.add(`Duplicate found: CardID ${card.cardid}, Variant ${card.pf_variant}, TCGP ID ${card.tcgp_id}`);
                } else {
                    uniqueCardVariants.add(key);
                }
            });

            //console.log(`Found ${cards.length} total card records`);
            //console.log(`Found ${uniqueCardVariants.size} unique card+variant combinations`);
            if (duplicates.size > 0) {
                //console.log('Duplicates:', Array.from(duplicates));
            }

            const productMap: ProductVariants = cards.reduce((acc, card) => {
                if (!acc[card.tcgp_id]) {
                    acc[card.tcgp_id] = {
                        variants: [],
                        cards: []
                    };
                }
                if (!acc[card.tcgp_id].variants.includes(card.tcgp_variant)) {
                    acc[card.tcgp_id].variants.push(card.tcgp_variant);
                    acc[card.tcgp_id].cards.push(card);
                }
                return acc;
            }, {} as ProductVariants);

            const productIds = Object.keys(productMap);
            let processedPrices = 0;
            let insertedPrices = 0;
            let failedInserts = 0;

            const CHUNK_SIZE = 50;
            for (let i = 0; i < productIds.length; i += CHUNK_SIZE) {
                const chunk = productIds.slice(i, i + CHUNK_SIZE);
                const url = `https://api.tcgplayer.com/pricing/product/${chunk.join(',')}`;

                try {
                    const response = await axios.get(url, {
                        headers: {
                            'Authorization': `Bearer ${token}`
                        }
                    });

                    const insertDataMap = new Map();
                    const now = new Date();
                    const updatedSource = now.toISOString().split('T')[0].replace(/-/g, '/');

                    for (const price of response.data.results) {
                        processedPrices++;
                        const productCards = productMap[price.productId.toString()];
                        if (!productCards) {
                            skippedCards.add(`No product found - ID: ${price.productId}`);
                            continue;
                        }

                        const matchingCard = productCards.cards.find(
                            card => card.tcgp_variant === price.subTypeName
                        );

                        if (!matchingCard) continue;

                        if (price.marketPrice === null) {
                            nullPriceCards.add(`Null price - Card: ${matchingCard.cardid}, Variant: ${matchingCard.pf_variant}`);
                            continue;
                        }

                        const key = `${matchingCard.cardid}-${matchingCard.pf_variant}`;
                        if (!insertDataMap.has(key)) {
                            insertDataMap.set(key, {
                                cardid: matchingCard.cardid,
                                variant: matchingCard.pf_variant,
                                price: price.marketPrice,
                                updatedsource: updatedSource,
                                updated: now,
                                source: 'tcgplayer'
                            });
                        }
                    }

                    const insertData = Array.from(insertDataMap.values());
                    if (insertData.length > 0) {
                        for (const record of insertData) {
                            try {
                                await db.none(`
                                    INSERT INTO pf_cards_price_history(cardid, variant, price, updatedsource, updated, source)
                                    SELECT $1, $2, $3, $4, $5, $6
                                    WHERE NOT EXISTS (
                                        SELECT 1 
                                        FROM pf_cards_price_history 
                                        WHERE cardid = $1 
                                        AND variant = $2 
                                        AND updatedsource = $4
                                    )
                                `, [record.cardid, record.variant, record.price, record.updatedsource, record.updated, record.source]);
                                insertedPrices++;
                            } catch (err) {
                                failedInserts++;
                                failedCards.add(`Failed to insert - Card: ${record.cardid}, Variant: ${record.variant}, Error: ${err.message}`);
                                console.error(`Failed to insert price for ${record.cardid} ${record.variant}: ${err.message}`);
                            }
                        }
                    }

                    //console.log(`Progress for set ${set.setid}: Processed ${processedPrices} prices, Inserted ${insertedPrices}, Failed ${failedInserts}`);
                    await new Promise(resolve => setTimeout(resolve, 100));

                } catch (error) {
                    console.error(`Error details for set ${set.setid}:`, error.response?.data || error.message);
                }
            }

            let setPrice;
            try {
                const setPriceResult = await db.one(`
                    SELECT SUM(price) as total_set_price 
                    FROM pf_cards_price_history pcph 
                    LEFT JOIN pfdata_cards pc ON pcph.cardid = pc.cardid 
                    WHERE pc.setid = $1 
                    AND updated::date = (
                        SELECT MAX(ph2.updated::date)
                        FROM pf_cards_price_history ph2
                        LEFT JOIN pfdata_cards pc2 ON ph2.cardid = pc2.cardid
                        WHERE pc2.setid = $1
                    )`, [set.setid]);
                setPrice = setPriceResult.total_set_price;
                //console.log(`Calculated set price for ${set.setid}: ${setPrice}`);
            } catch (err) {
                console.error(`Error calculating set price for ${set.setid}: ${err}`);
                continue;
            }

            if (setPrice) {
                const setPriceCs = new pgp.helpers.ColumnSet([
                    "setid", "price", "updatedsource", "source", {
                        name: 'updated',
                        def: () => new Date()
                    }
                ], { table: 'pfdata_setprices' });

                const setPriceOnConflict = ' ON CONFLICT(setid, source, updatedsource) DO NOTHING';
                let setPriceQuery = pgp.helpers.insert([{
                    setid: set.setid,
                    price: setPrice,
                    updatedsource: new Date(),
                    source: "tcgplayer"
                }], setPriceCs) + setPriceOnConflict;

                try {
                    await db.any(setPriceQuery);
                    //console.log(`Set price inserted for ${set.setid}`);
                } catch (err) {
                    console.error(`Error inserting set price for ${set.setid}: ${err}`);
                }
            }

            progressDetails[set.setid] = {
                processed: processedPrices,
                inserted: insertedPrices,
                failed: failedInserts,
                setPrice: setPrice,
                totalRecords: cards.length,
                uniqueRecords: uniqueCardVariants.size,
                duplicateCards: Array.from(duplicates),
                flaggedDetails: {
                    skippedCards: Array.from(skippedCards),
                    nullPriceCards: Array.from(nullPriceCards),
                    failedCards: Array.from(failedCards)
                }
            };

            flaggedCardsCount[set.setid] = skippedCards.size + nullPriceCards.size + failedCards.size;

            try {
                await db.none(`
                    UPDATE pf_logs_price_update 
                    SET progress = $1, 
                        progress_details = $2,
                        flagged_cards = $3
                    WHERE id = $4
                `, [Number(progress), progressDetails, flaggedCardsCount, logId]);
            } catch (err) {
                console.error('Failed to update log entry:', err);
            }
        }

        try {
            await db.none(`
                UPDATE pf_logs_price_update 
                SET time_ended = $1, 
                    progress = 100
                WHERE id = $2
            `, [new Date(), logId]);
        } catch (err) {
            console.error('Failed to update final log entry:', err);
        }

    } catch (error) {
        console.error('Fatal error:', error);
        throw error;
    }
}