import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
import AWSConfig from '../utils/aws_config';
import { String } from 'aws-sdk/clients/cloudhsm';

/** Logging Setup */
import { logToDBStart } from "../utils/logger";
const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, label, json } = format;

const aws = require('aws-sdk');
aws.config.update(AWSConfig());

// Set S3 endpoint to DigitalOcean Spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const s3 = new aws.S3({
  forcePathStyle: false,
  endpoint: spacesEndpoint
});

const pgp = require('pg-promise')(); //User Postgres Helpers
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance

//PokemonTCG API Result Interface
interface Set {
    id: string,
    name: string,
    series: string,
    printedTotal: number,
    total: number,
    legalities: object,
    ptcgoCode: string,
    releaseDate: string,
    updatedAt: string,
    images: setImage
}

interface setImage {
    symbol: string;
    logo: string;
}

interface collectSetsResponse {
    state: boolean,
    errors: number,
    log: number
}

export default async function CollectSets(db: any, metaFlag: boolean, imageFlag: boolean, method: string): Promise<collectSetsResponse> {
    let state: boolean = false;
    let responseSets: Array<Set> = [];
    let errors: number = 0;

    /*** Initialize Logging ***/
    let logid = await logToDBStart(db, "collectSets", method); //Send to start log to DB, return log id.
    console.log(logid)
    const logger = createLogger({
        levels: config.syslog.levels,
        format: combine(
            label({ label: 'collectSets' }),
            timestamp(),
            json()
          )  
         ,
        transports: [
          new transports.File({ filename: `logs/collectSets/${logid}.log`}),
        ],
    });
    logger.info(" --- STARTING --- ")

    logger.info("Fetching Sets from API")


    /*** Send HTTP Request and get Set data ***/
    await AxiosInstance.get("https://api.pokemontcg.io/v2/sets")
    .then( 
        async (response) => {
            responseSets = response.data.data;

             /*** Parse Data and Create Insert / Update Array ***/
             let insertArray = []; //Array used to collect meta data, later used for bulk insert.
             let imageArray = []; //Array used to collect image meta data, later used to insert into s3 bucket.

            responseSets.forEach(set => {
                insertArray.push(
                    {
                    setid: set.id,
                    name: set.name,
                    printedtotal: set.printedTotal,
                    total: set.total,
                    legalities: set.legalities,
                    ptcgocode: set.ptcgoCode,
                    releaseddate: set.releaseDate,
                    updatedat: set.updatedAt,
                    imgsymbol: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${set.id}/symbol.png`,
                    imglogo: `https://pokefolio.nyc3.cdn.digitaloceanspaces.com/images/${set.id}/logo.png`,
                    series: set.series
                    }
                )

                let symbolImageObj = {
                    downloadFrom: set.images.symbol,
                    uploadTo: `images/${set.id}`,
                    filename: 'symbol.png'
                }

                let logoImageObj = {
                    downloadFrom: set.images.logo,
                    uploadTo: `images/${set.id}`,
                    filename: 'logo.png'
                }

                imageArray.push(symbolImageObj);
                imageArray.push(logoImageObj);
            });
            logger.info("Fetching Sets from API - DONE")

            /*** Insert Into Database ***/
            if (metaFlag) {
                logger.info("Inserting metadata to database")

                const cs = new pgp.helpers.ColumnSet(["setid", "name", "printedtotal", "total", "legalities", "ptcgocode", "releaseddate", "updatedat", "imgsymbol", "imglogo", "series"], {table: 'pfdata_sets'})
                const onConflict = ' ON CONFLICT(setid) DO UPDATE SET ' + cs.assignColumns({from: "EXCLUDED", skip: ['setid']});
                let query = pgp.helpers.insert(insertArray, cs) + onConflict;
                let insert;

                try {
                    insert = await db.any(query);
                } catch (err) {
                    logger.error("Error inserting metadata to database", err);
                    errors++;
                }
                logger.info("Inserting metadata to database - DONE")
            }

            /*** Download Images and Upload to S3 Bucket ***/
            if (imageFlag) {
                logger.info("Uploading Images to Spaces")
                for (let i=0; i<imageArray.length; i++) {
                    let downloaded;
                    let put;
                    try {
                        downloaded = await axios.get(encodeURI(imageArray[i].downloadFrom), {
                            responseType: "arraybuffer"
                        })
                    }
                    catch (err) {
                        logger.error(`Failed to download image: ${imageArray[i].downloadFrom}`, err);
                        logger.error(`Skipping upload for current image: ${imageArray[i].downloadFrom}`, err);

                        errors++;
                        continue;
                    }
    
                    try {
                       put = await s3.putObject({
                            'ACL': 'public-read',
                            'Body': downloaded.data,
                            'Bucket': 'pokefolio',
                            'Key': `${imageArray[i].uploadTo}/${imageArray[i].filename}`,
                            'ContentType': 'image/png'
                        }, (err, data) => {
                            if (err) {
                                logger.error('Failed to put image into spaces: ', `${imageArray[i].uploadTo}/${imageArray[i].filename}`);
                                errors++;
                            }
                        })
                    } catch (err) {
                        logger.error('Failed to put image into spaces: ', `${imageArray[i].uploadTo}/${imageArray[i].filename}`);
                        errors++;
                    }
                }
                logger.info("Uploading Images to Spaces - DONE")
            }
            state = true;
        }
    )
    .catch(e => { // Error getting set data from API.
        logger.error("Error getting set data from API", e);
        errors++;
        state = false;
    }); 

    logger.info(" --- COMPLETE --- ")


    return new Promise<collectSetsResponse>((resolve, reject) => {
        if (state) { 
            resolve (
                {
                    state: state,
                    errors: errors,
                    log: logid
                })
        } else {
            reject (
                {
                    state: state,
                    errors: errors,
                    log: logid
                })
        }  
    })
}