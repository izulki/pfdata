

const pgp = require('pg-promise')(); //User Postgres Helpers

interface collectAnalysisResponse {
    state: boolean,
    errors: number,
    log: number
}


export default async function CollectAnalysis(db: any): Promise<collectAnalysisResponse> {
    //Collect The Top Gainer

    let data = await db.any(query, [true]);
    let prices = [];

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
                            change: change
                        }
                        prices.push(obj)
                    }
                } catch (e) {
                    console.log(`error: ${data[i].cardid}`)
                }

            }
        }
    }
    
    console.log(prices.sort((a,b) => b.change - a.change));

    return Promise.resolve({
        state: true,
        errors: 0,
        log: 0
    })

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