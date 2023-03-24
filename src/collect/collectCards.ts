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

        for (let j=1; j<=pages; j++) {
            await AxiosInstance.get(`https://api.pokemontcg.io/v2/cards?q=set.id:${sets[i].setid}&page=${j}`).then(async (response) => {
            console.log(response.data.data)




           
            
            
            console.log(`${sets[i].setid}: Page ${j} of ${pages}`)
        })
        }



    }

    return status;
}