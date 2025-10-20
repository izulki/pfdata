import BackgroundRemover, { ProductBackgroundRemover } from "../utils/clean_image";
import AWSConfig from '../utils/aws_config';
import axios from "axios";
import sharp from "sharp";

const aws = require('aws-sdk');
aws.config.update(AWSConfig());

// Set S3 endpoint to DigitalOcean Spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const s3 = new aws.S3({
    endpoint: spacesEndpoint
});

// Convert image to WebP format
async function convertToWebP(imageBuffer: Buffer, sourceFormat: 'png' | 'jpg', quality: number = 80): Promise<Buffer> {
    try {
        console.log(`🔄 Converting ${sourceFormat.toUpperCase()} to WebP...`);

        const webpBuffer = await sharp(imageBuffer)
            .webp({
                quality,
                effort: 4, // Better compression (0-6, higher = better compression but slower)
                lossless: false
            })
            .toBuffer();

        const originalSize = imageBuffer.length;
        const webpSize = webpBuffer.length;
        const savings = Math.round(((originalSize - webpSize) / originalSize) * 100);

        console.log(`✓ Converted to WebP: ${originalSize} bytes → ${webpSize} bytes (${savings}% smaller)`);

        return webpBuffer;
    } catch (error) {
        throw new Error(`Failed to convert image to WebP: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
}

// Example usage
export default async function collectSealedImages(db: any): Promise<boolean> {
    let sets = await db.any("SELECT setid, id FROM pfdata_sets ORDER BY id DESC", []);

    let allowedSets: string[] = [
        "other"
    ]; // Add specific setids here if you want to filter

    let processSets = allowedSets.length ? sets.filter(set => allowedSets.includes(set.setid)) : sets;

    for (let i = 0; i < processSets.length; i++) {
        console.log("Processing: ", processSets[i].setid);

        let productBatch = await db.query("select tcgp_id, setid from pf_sealed where setid = $1", [processSets[i].setid]);

        for (let j = 0; j < productBatch.length; j++) {
            console.log(`Processing product ${j + 1}/${productBatch.length}: ${productBatch[j].tcgp_id}`);

            try {
                // 1. Download the image
                const downloaded_image = await fetchImage(productBatch[j].tcgp_id);

                if (!downloaded_image) {
                    console.log(`❌ Failed to download image for product: ${productBatch[j].tcgp_id}`);
                    continue;
                }

                // 2. Convert to WebP
                console.log(`🔄 Converting ${downloaded_image.sourceFormat.toUpperCase()} to WebP...`);
                const webpBuffer = await convertToWebP(downloaded_image.buffer, downloaded_image.sourceFormat, 80);

                // 3. Upload the WebP image
                const uploadParams = {
                    'ACL': 'public-read',
                    'Body': webpBuffer,
                    'Bucket': 'pokefolio',
                    'Key': `images/${productBatch[j].setid}/sealed/${productBatch[j].tcgp_id}.webp`,
                    'ContentType': 'image/webp',
                    'CacheControl': 'public, max-age=31536000, immutable'
                };

                const uploadResult = await s3.upload(uploadParams).promise();
                console.log(`✅ Successfully uploaded: ${uploadResult.Location}`);

            } catch (err: any) {
                console.error(`❌ Error processing ${productBatch[j].tcgp_id}:`, err.message);
                // Continue with next product instead of failing entire batch
            }
        }
    }

    return true; // Changed from null to boolean as per return type
}

async function fetchImage(productId: string): Promise<{ buffer: Buffer; sourceFormat: 'png' | 'jpg' } | null> {
    const formats: Array<'png' | 'jpg'> = ['png', 'jpg'];
    let baseUrl = 'https://public.getcollectr.com/public-assets/products/product_';

    for (const format of formats) {
        try {
            const url = `${baseUrl}${productId}.${format}?optimizer=image&format=webp&width=1200&quality=70&strip=metadata`;

            console.log(`Trying to fetch: ${url}`);

            const response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 30000, // 30 second timeout
                validateStatus: (status) => status === 200,
            });

            if (response.status === 200 && response.data) {
                console.log(`✓ Found image in ${format.toUpperCase()} format`);
                return {
                    buffer: Buffer.from(response.data),
                    sourceFormat: format
                };
            }
        } catch (error) {
            if (axios.isAxiosError(error)) {
                if (error.response?.status === 404) {
                    console.log(`✗ Image not found in ${format.toUpperCase()} format`);
                } else {
                    console.log(`✗ Error fetching ${format.toUpperCase()}: ${error.message}`);
                }
            } else {
                console.log(`✗ Unexpected error fetching ${format.toUpperCase()}: ${error}`);
            }
        }
    }

    return null;
}