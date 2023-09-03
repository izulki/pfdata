

import axios from 'axios';
const maxNumber = 920;

interface CollectSpritesResponse {
    state: boolean,
    errors: number,
    log: number
}

import AWSConfig from '../utils/aws_config';
import PTCGConfig from '../utils/axios_ptcg_config';

const aws = require('aws-sdk');
aws.config.update(AWSConfig());


// Set S3 endpoint to DigitalOcean Spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const s3 = new aws.S3({
    endpoint: spacesEndpoint
});

//Axios Instance
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance



export default async function CollectSprites(): Promise<CollectSpritesResponse> {
    let state = false;
    let errors = 0;

    for (let i=883; i<maxNumber+1; i++) {
        let downloaded;
        let put;


        try {
            downloaded = await axios.get(encodeURI(`https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/other/showdown/${i}.gif`), {
                responseType: "arraybuffer"
            })
        }
        catch (err) {
            console.log(`Error Downloading Image ${i}`);
            console.log(err);
            errors++;
        }

        try {
            put = await s3.putObject({
                'ACL': 'public-read',
                'Body': downloaded.data,
                'Bucket': 'pokefolio',
                'Key': `images/sprites/${i}.gif`,
                'ContentType': 'image/gif'
            },
                (err, data) => {
                    if (err) {
                        console.log(`Error Uploading Image ${i}`);
                        console.log(err);
                        errors++;
                    }
                })
        } catch (err) {
            console.log(`Error Uploading Image ${i}`);
            console.log(err);
            errors++;
        }
    }

    return {
        state: true,
        errors: errors,
        log: 0
    }
}