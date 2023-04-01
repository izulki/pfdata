import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
import AWSConfig from '../utils/aws_config';


const aws = require('aws-sdk');
aws.config.update(AWSConfig());

// Set S3 endpoint to DigitalOcean Spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const s3 = new aws.S3({
  forcePathStyle: false,
  endpoint: spacesEndpoint
});



const pgp = require('pg-promise')();

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



export default async function CollectSets(db: any, metaFlag: boolean, imageFlag: boolean): Promise<boolean> {

    let state: boolean = false;
    let responseSets: Array<Set>;

    /*** Send HTTP Request and get Set data ***/
    console.log("CollectSets: Collecting Sets from API")
    await AxiosInstance.get("https://api.pokemontcg.io/v2/sets")
    .then( 
        async (response) => {
            responseSets = response.data.data;

             /*** Parse Data and Create Insert / Update Array ***/
             let insertArray = [];
             let imageArray = [];
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

            /*** Insert Into Database ***/
            if (metaFlag) {
                console.log("CollectSets: Insert Into Database")

                const cs = new pgp.helpers.ColumnSet(["setid", "name", "printedtotal", "total", "legalities", "ptcgocode", "releaseddate", "updatedat", "imgsymbol", "imglogo", "series"], {table: 'pfdata_sets'})
                const onConflict = ' ON CONFLICT(setid) DO UPDATE SET ' + cs.assignColumns({from: "EXCLUDED", skip: ['setid']});
                //console.log(insertArray)
                let query = pgp.helpers.insert(insertArray, cs) + onConflict;
                let insert = await db.any(query);
                
                console.log("CollectSets: inserted meta: ", insert)
            }

            /*** Download Images and Upload to S3 Bucket ***/
            if (imageFlag) {
                console.log("CollectSets: Uploading Images")
                for (let i=0; i<imageArray.length; i++) {
                    let downloaded;
                    let put;
                    try {
                        downloaded = await axios.get(encodeURI(imageArray[i].downloadFrom), {
                            responseType: "arraybuffer"
                        })
                    }
                    catch (err) {
                        console.log("CollectSets: image download error ", err)
                    }
    
                    //console.log(`Uploading to: ${imageArray[i].uploadTo}/${imageArray[i].filename}`)
                    try {
                       put = await s3.putObject({
                            'ACL': 'public-read',
                            'Body': downloaded.data,
                            'Bucket': 'pokefolio',
                            'Key': `${imageArray[i].uploadTo}/${imageArray[i].filename}`,
                            'ContentType': 'image/png'
                        }, (err, data) => {
                            if (err) {
                                console.log('CollectSets image upload error: ', `${imageArray[i].uploadTo}/${imageArray[i].filename}`)
                            }
                            //console.log("CollectSets image ", err)
                            //console.log(data)
                        })
                    } catch (err) {
                        console.log("CollectSets image upload error ", err)
                    }
                }
            }
            state = true;
        }
    )
    .catch(e => { // Error handling
        console.log(e)
        state = false;
    }); 


    return state;
}