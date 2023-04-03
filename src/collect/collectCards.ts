import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
import AWSConfig from '../utils/aws_config';

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

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

interface collectCardsResponse {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectCards(db: any, metaFlag: boolean, imageFlag: boolean, set: string, method: string): Promise<collectCardsResponse> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectCards", method); //Send to start log to DB, return log id.
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectCards' }),
            timestamp(),
            json()
        )
        ,
        transports: [
            new transports.File({ filename: `logs/collectCards/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")

    if (set == "") { //Set identifier is empty, find price for all cards.
        logger.info("Updating card data for ALL Cards.")

        /*** Get All Sets From Database ***/
        let sets = await db.any("SELECT setid, id FROM pfdata_sets ORDER BY id DESC", [true]);
        for (let i = 0; i < sets.length; i++) {
            let cardsInsertArray = [];
            let cardImageArray = [];

            //Calcualte amount of pages to iterate
            let pages = 1;
            try {
                let setData = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                pages = Math.ceil(setData.data.totalCount / 250);
            } catch (err) {
                logger.error(`Error Fetching https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}`);
                logger.error(err);
                errors++;
            }

            /** Go through each page of the results to prepare insert Arrays **/
            for (let j = 1; j <= pages; j++) {
                logger.info(`FETCH https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`)
                let cards = [];
                try {
                    let response = await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    cards = response.data.data
                } catch (err) {
                    logger.error(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`);
                    errors++;
                }

                logger.info(`Prepare Insert Array with Card and Image Data`)
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
            if (metaFlag) {
                logger.info(`Inserting Meta for Set to Database`)
                const cardsCs = new pgp.helpers.ColumnSet([
                    "cardid", "name", "supertype", "subtypes",
                    "level", "hp", "types", "evolvesFrom", "evolvesTo",
                    "rules", "ancientTrait", "abilities", "attacks",
                    "weaknesses", "resistances", "retreatCost", "convertedRetreatCost",
                    "set", "number", "artist", "rarity", "flavorText", "nationalPokedexNumbers",
                    "legalities", "regulationMark", "images", "tcgplayer", "cardmarket", "setid"], { table: 'pfdata_cards' })
                const cardsOnConflict = ' ON CONFLICT(cardid) DO UPDATE SET ' + cardsCs.assignColumns({ from: "EXCLUDED", skip: ['cardid'] });
                let cardsQuery = pgp.helpers.insert(cardsInsertArray, cardsCs) + cardsOnConflict;
                let insertCards = await db.any(cardsQuery);
            }

            /** Insert Images into S3 Bucket  **/
            if (imageFlag) {
                logger.info(`Inserting Images for Set to Database`)
                for (let k = 0; k < cardImageArray.length; k++) {
                    let downloaded;
                    let put;
                    try {
                        downloaded = await axios.get(encodeURI(cardImageArray[k].downloadFrom), {
                            responseType: "arraybuffer"
                        })
                    }
                    catch (err) {
                        logger.error(`Error Downloading Image ${cardImageArray[k].downloadFrom}`);
                        logger.error(err);
                        errors++;
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
                                    logger.error(`Error Uploading Image ${cardImageArray[k].uploadTo}/${cardImageArray[k].filename}`);
                                    logger.error(err);
                                    errors++;
                                }
                            })
                    } catch (err) {
                        logger.error(`Error Uploading Image ${cardImageArray[k].uploadTo}/${cardImageArray[k].filename}`);
                        logger.error(err);
                        errors++;
                    }
                }
            }
        }
    } else {

    }

    logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectCards/${logid}.log`)
    logger.info(" --- COMPLETED --- ")

    let result: collectCardsResponse = {
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