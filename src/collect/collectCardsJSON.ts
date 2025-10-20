import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
import AWSConfig from '../utils/aws_config';

/** Logging Setup */
import { logToDBStart, logToDBEnd } from "../utils/logger";
const sharp = require('sharp');
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;
const fs = require('fs').promises;
const path = require('path');

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

export default async function CollectCardsJSON(
    db: any,
    metaFlag: boolean,
    imageFlag: boolean,
    set: string = "", // Default empty string
    method: string = "default", // Default method
    jsonFilePath: string = "" // New parameter for JSON file path
): Promise<collectCardsResponse> {
    let state = false;
    let errors = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectCardsJSON", method);
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectCardsJSON' }),
            timestamp(),
            json()
        ),
        transports: [
            new transports.File({ filename: `logs/collectCardsJSON/${logid}.log` }),
        ],
    });

    logger.info(" --- STARTING --- ")

    if (jsonFilePath != "") {
        // NEW: Process from JSON file
        logger.info(`Processing cards from JSON file: ${jsonFilePath}`);
        
        try {
            const jsonData = await fs.readFile(jsonFilePath, 'utf8');
            const cards = JSON.parse(jsonData);
            
            await processCardsFromJSON(cards, db, logger, metaFlag, imageFlag, errors, set);
        } catch (err) {
            logger.error(`Error reading or parsing JSON file: ${jsonFilePath}`);
            logger.error(err);
            errors++;
        }
    } 
    else {
        logger.error("No JSON file path provided. Please specify jsonFilePath parameter.");
        errors++;
    }

    await logToDBEnd(db, logid, "COMPLETED", errors, `/logs/collectCardsJSON/${logid}.log`);
    logger.info(" --- COMPLETED --- ")

    let result: collectCardsResponse = {
        state: state,
        errors: errors,
        log: logid
    }

    if (errors == 0) {
        result.state = true;
        return Promise.resolve(result)
    } else {
        result.state = false;
        return Promise.reject(result)
    }
}

// NEW FUNCTION: Process cards from JSON data
async function processCardsFromJSON(
    cards: any[], 
    db: any, 
    logger: any, 
    metaFlag: boolean, 
    imageFlag: boolean, 
    errors: number,
    setId: string = ""
) {
    let cardsInsertArray = [];
    let cardImageArray = [];

    logger.info(`Processing ${cards.length} cards from JSON data`);

    // Extract set info from first card if setId not provided
    if (!setId && cards.length > 0) {
        // Extract set ID from first card's ID (e.g., "me1-1" -> "me1")
        setId = cards[0].id.split('-')[0];
    }

    // Get set information from database
    let setObject = {};
    try {
        let setData = await db.oneOrNone("SELECT * FROM pfdata_sets WHERE setid = $1", [setId]);
        if (setData) {
            // Use database set information
            setObject = {
                id: setData.setid,
                name: setData.name,
                series: setData.series,
                printedTotal: setData.printedtotal,
                total: setData.total,
                legalities: setData.legalities || {},
                releaseDate: setData.releasedate,
                updatedAt: setData.updatedat || new Date().toISOString(),
                images: setData.images || {
                    symbol: "",
                    logo: ""
                }
            };
            logger.info(`Found set in database: ${setData.name}`);
        } else {
            // Fallback to basic set object if not found in database
            logger.warn(`Set ${setId} not found in database, using basic set info`);
            setObject = {
                id: setId,
                name: setId, // Fallback to ID as name
                series: "Unknown",
                printedTotal: 0,
                total: 0,
                legalities: {},
                releaseDate: "",
                updatedAt: new Date().toISOString(),
                images: {
                    symbol: "",
                    logo: ""
                }
            };
        }
    } catch (err) {
        logger.error(`Error querying database for set ${setId}`);
        logger.error(err);
        errors++;
        // Use fallback set object
        setObject = {
            id: setId,
            name: setId,
            series: "Unknown",
            printedTotal: 0,
            total: 0,
            legalities: {},
            releaseDate: "",
            updatedAt: new Date().toISOString(),
            images: {
                symbol: "",
                logo: ""
            }
        };
    }

    cards.forEach(card => {

        cardsInsertArray.push({
            cardid: card.id,
            name: card.name,
            supertype: card.supertype,
            subtypes: card.subtypes,
            level: card.level || null, // May be added to JSON in future
            hp: card.hp,
            types: card.types,
            evolvesFrom: card.evolvesFrom || null, // May be added to JSON in future
            evolvesTo: card.evolvesTo,
            rules: card.rules || null, // May be added to JSON in future
            ancientTrait: card.ancientTrait || null, // May be added to JSON in future
            abilities: card.abilities || null, // May be added to JSON in future
            attacks: card.attacks,
            weaknesses: card.weaknesses,
            resistances: card.resistances || null, // May be added to JSON in future
            retreatCost: card.retreatCost,
            convertedRetreatCost: card.convertedRetreatCost,
            set: setObject,
            number: card.number,
            artist: card.artist || null, // May be added to JSON in future
            rarity: card.rarity,
            flavorText: card.flavorText || null, // May be added to JSON in future
            nationalPokedexNumbers: card.nationalPokedexNumbers,
            legalities: card.legalities,
            regulationMark: card.regulationMark,
            images: {
                small: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${setId}/${card.number}.png`,
                large: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${setId}/${card.number}_hires.png`,
                small_webp: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${setId}/${card.number}.webp`,
                large_webp: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${setId}/${card.number}_hires.webp`
            },
            tcgplayer: card.tcgplayer || null, // May be added to JSON in future
            cardmarket: card.cardmarket || null, // May be added to JSON in future
            setid: setId
        });

        // Use the original image URLs from JSON
        let imageObj = {
            downloadFrom: card.images.small,
            uploadTo: `images/${setId}`,
            filename: `${card.number}.png`
        };

        let imageHiresObj = {
            downloadFrom: card.images.large,
            uploadTo: `images/${setId}`,
            filename: `${card.number}_hires.png`
        };

        cardImageArray.push(imageObj);
        cardImageArray.push(imageHiresObj);
    });

    /** Insert Data into Cards Table **/
    if (metaFlag) {
        logger.info(`Inserting Meta for Set to Database`);
        const cardsCs = new pgp.helpers.ColumnSet([
            "cardid", "name", "supertype", "subtypes",
            "level", "hp", "types", "evolvesFrom", "evolvesTo",
            "rules", "ancientTrait", "abilities", "attacks",
            "weaknesses", "resistances", "retreatCost", "convertedRetreatCost",
            "set", "number", "artist", "rarity", "flavorText", "nationalPokedexNumbers",
            "legalities", "regulationMark", "images", "tcgplayer", "cardmarket", "setid"
        ], { table: 'pfdata_cards' });
        
        const cardsOnConflict = ' ON CONFLICT(cardid) DO UPDATE SET ' + cardsCs.assignColumns({ from: "EXCLUDED", skip: ['cardid'] });
        let cardsQuery = pgp.helpers.insert(cardsInsertArray, cardsCs) + cardsOnConflict;
        let insertCards = await db.any(cardsQuery);
    }

    /** Insert Images into S3 Bucket **/
    if (imageFlag) {
        logger.info(`Inserting Images for Set to S3`);
        await processImages(cardImageArray, logger, s3, errors);
    }
}

// Extracted image processing function for reuse
async function processImages(cardImageArray: any[], logger: any, s3: any, errors: number) {
    for (let k = 0; k < cardImageArray.length; k++) {
        let downloaded;
        try {
            downloaded = await axios.get(encodeURI(cardImageArray[k].downloadFrom), {
                responseType: "arraybuffer"
            });
        } catch (err) {
            logger.error(`Error Downloading Image ${cardImageArray[k].downloadFrom}`);
            logger.error(err);
            errors++;
            continue;
        }

        try {
            // 1. Upload the original PNG
            await s3.putObject({
                'ACL': 'public-read',
                'Body': downloaded.data,
                'Bucket': 'pokefolio',
                'Key': `${cardImageArray[k].uploadTo}/${cardImageArray[k].filename}`,
                'ContentType': 'image/png',
                'CacheControl': 'public, max-age=31536000, immutable'
            }).promise();

            // 2. Convert to WebP and upload that version
            const webpBuffer = await sharp(downloaded.data)
                .webp({
                    quality: 80,
                    lossless: false,
                    nearLossless: false,
                    smartSubsample: true,
                    reductionEffort: 4
                })
                .toBuffer();

            const webpFilename = cardImageArray[k].filename.includes('.png')
                ? cardImageArray[k].filename.replace(/\.png$/i, '.webp')
                : `${cardImageArray[k].filename}.webp`;

            await s3.putObject({
                'ACL': 'public-read',
                'Body': webpBuffer,
                'Bucket': 'pokefolio',
                'Key': `${cardImageArray[k].uploadTo}/${webpFilename}`,
                'ContentType': 'image/webp',
                'CacheControl': 'public, max-age=31536000, immutable'
            }).promise();

        } catch (err) {
            logger.error(`Error Processing or Uploading Image ${cardImageArray[k].uploadTo}/${cardImageArray[k].filename}`);
            logger.error(err);
            errors++;
        }
    }
}