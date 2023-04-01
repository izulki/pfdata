import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
import AWSConfig from '../utils/aws_config';


const aws = require('aws-sdk');
aws.config.update(AWSConfig());

// Set S3 endpoint to DigitalOcean Spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const s3 = new aws.S3({
  endpoint: spacesEndpoint
});

// Database Instance
const pgp = require('pg-promise')();

//Axios Instance
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance

export default async function CollectCards(db: any, metaFlag: boolean, imageFlag: boolean): Promise<boolean> {
    let status = false;

    /*** Get All Sets From Database ***/
    let sets = await db.any("SELECT setid, id FROM pfdata_sets ORDER BY id DESC", [true]);

    /*** Get Cards for each set ***/
    for (let i=0; i<sets.length; i++) {
        console.log(`CollectCards Processing ${sets[i].setid}`)
        let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
        let pages = Math.ceil(setData.data.totalCount/250);

        let cardsInsertArray = [];
        let cardPriceInsertArray = [];
        let cardImageArray = [];

        /** Go through each page of the results to prepare insert Arrays **/
        for (let j=1; j<=pages; j++) { 
            let cards = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`).then(async (response) => {
                return response.data.data
            })


            /** Go through each card of each page **/
            cards.forEach(card => {
                cardsInsertArray.push(
                    {
                        cardid: card.id,
                        name: card.name,
                        supertype: card.supertype,
                        subtypes: card.subtypes,
                        level: card.level,
                        hp: card.hp,
                        types: card.types,
                        evolvesFrom: card.evolvesFrom,
                        evolvesTo: card.evolvesTo,
                        rules: card.rules,
                        ancientTrait: card.ancientTrait,
                        abilities: card.abilities,
                        attacks: card.attacks,
                        weaknesses: card.weaknesses,
                        resistances: card.resistances,
                        retreatCost: card.retreatCost,
                        convertedRetreatCost: card.convertedRetreatCost,
                        set: card.set,
                        number: card.number,
                        artist: card.artist,
                        rarity: card.rarity,
                        flavorText: card.flavorText,
                        nationalPokedexNumbers: card.nationalPokedexNumbers,
                        legalities: card.legalities,
                        regulationMark: card.regulationMark,
                        images: {
                            small: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${card.set.id}/${card.number}.png`,
                            large: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${card.set.id}/${card.number}_hires.png`
                        },
                        tcgplayer: card.tcgplayer,
                        cardmarket: card.cardmarket,
                        setid: card.set.id
                    }
                )

                cardPriceInsertArray.push(
                    {
                        cardid: card.id,
                        source: "tcgplayer",
                        prices: "tcgplayer" in card ? card.tcgplayer.prices : null,
                        updatedsource: "tcgplayer" in card ? card.tcgplayer.updatedAt : null,
                    }
                )

                let imageObj = {
                    downloadFrom: card.images.small,
                    uploadTo: `images/${card.set.id}`,
                    filename: `${card.number}.png`
                }

                let imageHiresObj = {
                    downloadFrom: card.images.large,
                    uploadTo: `images/${card.set.id}`,
                    filename: `${card.number}_hires.png`
                }

                cardImageArray.push(imageObj)
                cardImageArray.push(imageHiresObj)
            })
        }

        /** Insert Data into Cards Table  **/
        //console.log(cardsInsertArray)
        if (metaFlag) {
            console.log("CollectCards: Insert Into Database")
            const cardsCs = new pgp.helpers.ColumnSet([
                "cardid", "name", "supertype", "subtypes", 
                "level", "hp", "types", "evolvesFrom", "evolvesTo", 
                "rules", "ancientTrait", "abilities", "attacks", 
                "weaknesses", "resistances", "retreatCost", "convertedRetreatCost", 
                "set", "number", "artist", "rarity", "flavorText", "nationalPokedexNumbers",
                "legalities", "regulationMark", "images", "tcgplayer", "cardmarket", "setid"], {table: 'pfdata_cards'})
            const cardsOnConflict = ' ON CONFLICT(cardid) DO UPDATE SET ' + cardsCs.assignColumns({from: "EXCLUDED", skip: ['cardid']});
            let cardsQuery = pgp.helpers.insert(cardsInsertArray, cardsCs) + cardsOnConflict;
            let insertCards = await db.any(cardsQuery);
            console.log("CollectCards: inserted meta: ", insertCards)
        }

        /** Insert Data into Price Table  **/
        console.log("CollectCards: Insert PRICE Into Database")
        const priceCs = new pgp.helpers.ColumnSet([
            "cardid", "source", "prices", "updatedsource", {
                name: 'updated',
                def: () => new Date() // default to the current Date/Time
            }
        ], {table: 'pfdata_cardprices'});
        const pricesOnConflict = ' ON CONFLICT(cardid, source, updatedsource) DO NOTHING';

        let priceQuery = pgp.helpers.insert(cardPriceInsertArray, priceCs) + pricesOnConflict;
        let insertPrices = await db.any(priceQuery);
        console.log("CollectCards: inserted prices: ", insertPrices)

        /** Insert Images into S3 Bucket  **/
        if (imageFlag) {
            console.log("CollectCards: Uploading Images")
            for (let k=0; k<cardImageArray.length; k++) {
                let downloaded;
                let put;
                try {
                    downloaded = await axios.get(encodeURI(cardImageArray[k].downloadFrom), {
                        responseType: "arraybuffer"
                    })
                }
                catch (err) {
                    console.log("CollectCards: image download error ", cardImageArray[k].downloadFrom, err)
                }
                try {
                   put = await s3.putObject({
    
                        'ACL': 'public-read',
                        'Body': downloaded.data,
                        'Bucket': 'pokefolio',
                        'Key': `${cardImageArray[k].uploadTo}/${cardImageArray[k].filename}`,
                        'ContentType': 'image/png'
    
                    }, 
                    (err, data) => {
                        if (err) {
                            console.log('CollectCards image upload error: ', `${cardImageArray[i].uploadTo}/${cardImageArray[i].filename}`)
                        }                
                    })
                } catch (err) {
                    console.log("CollectCards image upload error ", err)
                }
            }
        }
    }
    status = true;
    return status;
}