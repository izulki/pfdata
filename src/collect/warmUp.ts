import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';

const pgp = require('pg-promise')();
const AxiosInstance = axios.create(PTCGConfig());

// Configuration constants
const MAX_IMAGES_PER_PAGE = 25;
const IMAGE_TIMEOUT = 5000;
const BATCH_SIZE = 3;
const BATCH_DELAY = 2000;
const RETRY_DELAY = 5000;

async function extractImageUrls(html: string): Promise<string[]> {
    const imgRegex = /https:\/\/[^"']*\.(?:png|jpg|jpeg|webp|avif)/g;
    const matches = html.match(imgRegex) || [];
    const uniqueUrls = Array.from(new Set(matches)); // Remove duplicates
    
    // Filter out _hires images
    const filteredUrls = uniqueUrls.filter(url => !url.includes('_hires'));
    
    return filteredUrls.slice(0, MAX_IMAGES_PER_PAGE); // Limit to first 25 non-hires images
}

async function warmUpImage(imageUrl: string): Promise<{ url: string; status: string; error?: string }> {
    try {
        await axios.get(imageUrl, { 
            method: 'HEAD',
            timeout: IMAGE_TIMEOUT
        });
        //console.log(`Warmed up image: ${imageUrl}`);
        return { url: imageUrl, status: 'success' };
    } catch (error: any) {
        console.error(`Failed to warm up image ${imageUrl}:`, error.message);
        return { url: imageUrl, status: 'error', error: error.message };
    }
}

async function warmUpPage(setid: string): Promise<{ 
    setid: string; 
    status: string; 
    error?: string;
    imageResults?: Array<{ url: string; status: string; error?: string }>;
}> {
    const url = `https://www.pokefolio.co/sets/${setid}`;
    try {
        const response = await AxiosInstance.get(url);
        const html = response.data;
        
        // Extract and warm up first 25 images
        const imageUrls = await extractImageUrls(html);
        console.log(`Found ${imageUrls.length} images for ${url} (limited to first ${MAX_IMAGES_PER_PAGE})`);
        
        const imageResults = await Promise.all(
            imageUrls.map(imageUrl => warmUpImage(imageUrl))
        );

        //console.log(`Successfully warmed up: ${url} with ${imageUrls.length} images`);
        return { 
            setid, 
            status: 'success',
            imageResults
        };
    } catch (error: any) {
        if (error.response?.status === 504) {
            console.log(`504 timeout for ${url} - page will be cached for next visit`);
            return { setid, status: 'timeout_success' };
        }
        console.error(`Failed to warm up ${url}:`, error.message);
        return { setid, status: 'error', error: error.message };
    }
}

export default async function WarmUp(db: any) {
    try {
        const sets = await db.any("SELECT setid FROM pfdata_sets ORDER BY releaseddate DESC");
        
        console.log(`Starting warm-up process for ${sets.length} sets...`);
        
        const warmupPromises = sets.map(set => ({ setid: set.setid }));
        const results = [];
        
        // First pass through all pages
        for (let i = 0; i < warmupPromises.length; i += BATCH_SIZE) {
            const batch = warmupPromises.slice(i, i + BATCH_SIZE);
            const batchResults = await Promise.all(
                batch.map(set => warmUpPage(set.setid))
            );
            results.push(...batchResults);
            
            if (i + BATCH_SIZE < warmupPromises.length) {
                await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
            }
        }

        // Wait before retrying errors
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));

        // Second pass - retry genuine errors
        const failedPages = results
            .filter(r => r.status === 'error')
            .map(r => r.setid);

        if (failedPages.length > 0) {
            console.log(`Retrying ${failedPages.length} failed pages...`);
            
            for (const setid of failedPages) {
                const retryResult = await warmUpPage(setid);
                const index = results.findIndex(r => r.setid === setid);
                if (index !== -1) {
                    results[index] = retryResult;
                }
                await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
            }
        }

        // Compile statistics
        const successful = results.filter(r => r.status === 'success').length;
        const timeoutSuccess = results.filter(r => r.status === 'timeout_success').length;
        const failed = results.filter(r => r.status === 'error').length;
        
        // Compile image statistics
        const imageStats = {
            total: 0,
            successful: 0,
            failed: 0
        };

        results.forEach(result => {
            if (result.imageResults) {
                imageStats.total += result.imageResults.length;
                imageStats.successful += result.imageResults.filter(r => r.status === 'success').length;
                imageStats.failed += result.imageResults.filter(r => r.status === 'error').length;
            }
        });

        console.log(`Warm-up process completed:`);
        console.log(`Pages:`);
        console.log(`- Successfully warmed up: ${successful} pages`);
        console.log(`- Timeout (will be cached): ${timeoutSuccess} pages`);
        console.log(`- Failed to warm up: ${failed} pages`);
        console.log(`Images:`);
        console.log(`- Total images processed: ${imageStats.total}`);
        console.log(`- Successfully warmed up: ${imageStats.successful} images`);
        console.log(`- Failed to warm up: ${imageStats.failed} images`);

        return {
            pages: {
                total: sets.length,
                successful,
                timeoutSuccess,
                failed,
            },
            images: imageStats,
            results
        };

    } catch (error) {
        console.error('Error during warm-up process:', error);
        throw error;
    }
}