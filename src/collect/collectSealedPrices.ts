const pgp = require('pg-promise')();
import axios from 'axios';
import { getAuthToken } from '../utils/getAuthToken';
import dayjs from 'dayjs';
require('dotenv').config();

interface SealedPriceRecord {
    sealedid: string;
    tcgp_id: number;
    name: string;
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
        cards: SealedPriceRecord[];
    }
}

export async function collectSealedPrices(db: any): Promise<void> {
    try {
        const sets = await db.any("SELECT setid FROM pfdata_sets ORDER BY releaseddate DESC");
        const token = await getAuthToken();
        
        const totalSets = sets.length;
        const timeStarted = new Date();
        
        let logId: number;
        try {
            const result = await db.one(`
                INSERT INTO pf_logs_sealed_price_update (progress, time_started, progress_details, flagged_products)
                VALUES (0, $1, '{}', '{}')
                RETURNING id
            `, [timeStarted]);
            logId = result.id;
        } catch (err) {
            console.error('Failed to create sealed price update log entry:', err);
            throw err;
        }

        const progressDetails: { [key: string]: any } = {};
        const flaggedProductsCount: { [key: string]: number } = {};

        for (let setIndex = 0; setIndex < sets.length; setIndex++) {
            const set = sets[setIndex];
            const progress = ((setIndex + 1) / totalSets * 100).toFixed(1);
            //console.log(`\nProcessing set ${set.setid} (${setIndex + 1}/${totalSets} - ${progress}%)`);

            const query = `
                SELECT sealedid, tcgp_id, name, setid 
                FROM pf_sealed 
                WHERE setid = $1 AND tcgp_id IS NOT NULL
            `;
            const sealedProducts: SealedPriceRecord[] = await db.any(query, [set.setid]);
            
            if (sealedProducts.length === 0) {
                //console.log(`No sealed products found for set ${set.setid}`);
                continue;
            }

            const productIds = sealedProducts.map(product => product.tcgp_id.toString());

            let processedPrices = 0;   
            let insertedPrices = 0;      
            let failedInserts = 0;     
            
            const skippedProducts = new Set();
            const nullPriceProducts = new Set();
            const failedProducts = new Set();

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
                        
                        // Find the matching sealed product
                        const matchingProduct = sealedProducts.find(
                            product => product.tcgp_id === price.productId
                        );

                        if (!matchingProduct) {
                            skippedProducts.add(`No product found - TCG ID: ${price.productId}`);
                            continue;
                        }

                        if (price.marketPrice === null) {
                            nullPriceProducts.add(`Null price - Product: ${matchingProduct.name}, TCG ID: ${matchingProduct.tcgp_id}`);
                            continue;
                        }

                        // Use sealedid as the unique key (no variants for sealed products)
                        if (!insertDataMap.has(matchingProduct.sealedid)) {
                            insertDataMap.set(matchingProduct.sealedid, {
                                sealedid: matchingProduct.sealedid,
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
                                    INSERT INTO pf_sealed_price_history(sealedid, price, updatedsource, updated, source)
                                    SELECT $1, $2, $3, $4, $5
                                    WHERE NOT EXISTS (
                                        SELECT 1 
                                        FROM pf_sealed_price_history 
                                        WHERE sealedid = $1 
                                        AND updatedsource = $3
                                    )
                                `, [record.sealedid, record.price, record.updatedsource, record.updated, record.source]);
                                insertedPrices++;
                            } catch (err) {
                                failedInserts++;
                                const productName = sealedProducts.find(p => p.sealedid === record.sealedid)?.name || 'Unknown';
                                failedProducts.add(`Failed to insert - Product: ${productName}, SealedID: ${record.sealedid}, Error: ${err.message}`);
                                console.error(`Failed to insert price for sealed product ${record.sealedid}: ${err.message}`);
                            }
                        }
                    }

                    //console.log(`Progress for set ${set.setid}: Processed ${processedPrices} prices, Inserted ${insertedPrices}, Failed ${failedInserts}`);
                    await new Promise(resolve => setTimeout(resolve, 100));

                } catch (error) {
                    console.error(`Error details for set ${set.setid}:`, error.response?.data || error.message);
                }
            }
     }

        try {
            await db.none(`
                UPDATE pf_logs_sealed_price_update 
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