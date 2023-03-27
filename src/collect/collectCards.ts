import axios from 'axios';
import { arrayBuffer } from 'stream/consumers';
import PTCGConfig from '../utils/axios_ptcg_config';
import DBConfig from '../utils/db_config';
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
const db = pgp(DBConfig());

//Axios Instance
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance




export default async function CollectCards(): Promise<boolean> {
    let status = false;

    /*** Get All Sets From Database ***/
    let sets = await db.any("SELECT setid FROM pfdata_sets", [true]);

    /*** Get Cards for each set ***/
    for (let i=0; i<sets.length; i++) {
        console.log(`Processing ${sets[i].setid}`)
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
                        images: card.images,
                        tcgplayer: card.tcgplayer,
                        cardmarket: card.cardmarket
                    }
                )

                cardPriceInsertArray.push(
                    {
                        cardid: card.id,
                        source: "tcgplayer",
                        prices: card.tcgplayer.prices,
                        updatedsource: card.tcgplayer.updatedAt,
                    }
                )
            })
        }

        /** Insert Data into Cards Table  **/
        //console.log(cardsInsertArray)

        const cardsCs = new pgp.helpers.ColumnSet([
            "cardid", "name", "supertype", "subtypes", 
            "level", "hp", "types", "evolvesFrom", "evolvesTo", 
            "rules", "ancientTrait", "abilities", "attacks", 
            "weaknesses", "resistances", "retreatCost", "convertedRetreatCost", 
            "set", "number", "artist", "rarity", "flavorText", "nationalPokedexNumbers",
            "legalities", "regulationMark", "images", "tcgplayer", "cardmarket"], {table: 'pfdata_cards'})
        const cardsOnConflict = ' ON CONFLICT(cardid) DO UPDATE SET ' + cardsCs.assignColumns({from: "EXCLUDED", skip: ['cardid']});
        let cardsQuery = pgp.helpers.insert(cardsInsertArray, cardsCs) + cardsOnConflict;
        let insertCards = await db.any(cardsQuery);
        console.log(insertCards)


        /** Insert Data into Price Table  **/
        const priceCs = new pgp.helpers.ColumnSet([
            "cardid", "source", "prices", "updatedsource", {
                name: 'updated',
                def: () => new Date() // default to the current Date/Time
            }
        ], {table: 'pfdata_cardprices'});
        const pricesOnConflict = ' ON CONFLICT(cardid, source, updatedsource) DO NOTHING';


        let priceQuery = pgp.helpers.insert(cardPriceInsertArray, priceCs) + pricesOnConflict;
        let insertPrices = await db.any(priceQuery);
        console.log(insertPrices);

        /** Insert Data into S3 Bucket  **/





    }

    return status;
}