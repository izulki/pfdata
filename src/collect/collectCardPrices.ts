const pgp = require('pg-promise')(); //User Postgres Helpers
import axios from 'axios';
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
        const sets = await db.any("SELECT setid FROM pfdata_sets");
        //console.log(`Found ${sets.length} sets to process`);

        for (const set of sets) {
            //console.log(`Processing set: ${set.setid}`);
            
            const query = `
                SELECT pcpm.*, pc.setid 
                FROM pf_cards_pricing_map pcpm
                LEFT JOIN pfdata_cards pc ON pc.cardid = pcpm.cardid
                WHERE pc.setid = $1
            `;
            const cards: PriceRecord[] = await db.any(query, [set.setid]);
            //console.log(`Found ${cards.length} cards for set ${set.setid}`);

            if (cards.length === 0) {
                //console.log(`No cards found for set: ${set.setid}`);
                continue;
            }

            const productMap: ProductVariants = cards.reduce((acc, card) => {
                if (!acc[card.tcgp_id]) {
                    acc[card.tcgp_id] = {
                        variants: [],
                        cards: []
                    };
                }
                acc[card.tcgp_id].variants.push(card.tcgp_variant);
                acc[card.tcgp_id].cards.push(card);
                return acc;
            }, {} as ProductVariants);

            const productIds = Object.keys(productMap);
            //console.log(`Found ${productIds.length} unique product IDs for set ${set.setid}`);
            
            const CHUNK_SIZE = 50;
            for (let i = 0; i < productIds.length; i += CHUNK_SIZE) {
                const chunk = productIds.slice(i, i + CHUNK_SIZE);
                const url = `https://api.tcgplayer.com/pricing/product/${chunk.join(',')}`;
                //console.log(`Processing chunk ${i / CHUNK_SIZE + 1} of ${Math.ceil(productIds.length / CHUNK_SIZE)} for set ${set.setid}`);

                try {
                    const response = await axios.get(url, {
                        headers: {
                            'Authorization': `Bearer ${process.env.TCGP_API}`
                        }
                    });
                    //console.log(`Received ${response.data.results.length} price results from TCGPlayer`);

                    const insertData = [];
                    const now = new Date();
                    const updatedSource = now.toISOString().split('T')[0].replace(/-/g, '/');

                    for (const price of response.data.results) {
                        const productCards = productMap[price.productId.toString()];
                        if (!productCards) {
                            //console.log(`No matching product found for ID: ${price.productId}`);
                            continue;
                        }

                        const matchingCard = productCards.cards.find(
                            card => card.tcgp_variant === price.subTypeName
                        );

                        if (matchingCard && price.marketPrice !== null) {
                            insertData.push({
                                cardid: matchingCard.cardid,
                                variant: matchingCard.pf_variant,
                                price: price.marketPrice,
                                updatedsource: updatedSource,
                                updated: now,
                                source: 'tcgplayer'
                            });
                        } else {
                            //console.log(`No matching variant found for product ${price.productId}, subType: ${price.subTypeName}`);
                        }
                    }

                    //console.log(`Preparing to insert ${insertData.length} price records`);
                    
                    if (insertData.length > 0) {
                        const cs = new pgp.helpers.ColumnSet([
                            'cardid',
                            'variant',
                            'price',
                            'updatedsource',
                            'updated',
                            'source'
                        ], {table: 'pf_cards_price_history'});

                        //console.log(insertData)

                        const query = pgp.helpers.insert(insertData, cs);
                        await db.none(query);
                        //console.log(`Successfully inserted ${insertData.length} records`);
                    }

                    await new Promise(resolve => setTimeout(resolve, 100));

                } catch (error) {
                    console.error(`Error details:`, error.response?.data || error.message);
                    //console.error(`Error processing chunk starting with product ${chunk[0]} for set ${set.setid}`);
                }
            }
        }

    } catch (error) {
        //console.error('Error in collectCardPrices:', error);
        throw error;
    }
}