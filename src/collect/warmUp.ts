import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';

const pgp = require('pg-promise')();
const AxiosInstance = axios.create(PTCGConfig());

// Configuration constants
const MAX_IMAGES_PER_PAGE = 25;
const IMAGE_TIMEOUT = 5000;
const API_TIMEOUT = 60000;  // Longer timeout for APIs
const PAGE_TIMEOUT = 15000; // Longer timeout for pages
const BATCH_SIZE = 3;
const BATCH_DELAY = 2000;
const RETRY_DELAY = 5000;

// Define the critical API endpoints needed for each set
const getSetApiEndpoints = (setid: string): string[] => {
  return [
    `set/${setid}`,           // Basic set info
    `pages/set/${setid}`,     // Cards in the set
    `pages/set/${setid}/uncollated`,    // Price data
    `price/set/chart/${setid}/30`
  ];
};

// Phase 1: Warm up API endpoints
async function warmUpApiEndpoint(endpoint: string): Promise<{ 
    endpoint: string; 
    status: string; 
    responseTime?: number;
    error?: string;
}> {
    const url = `https://api.pokefolio.co/api/v2/${endpoint}`;
    const startTime = Date.now();
    
    try {
        await AxiosInstance.get(url, {
            timeout: API_TIMEOUT,
            headers: { 
                'x-warming-request': 'true',
                'Cache-Control': 'no-cache'  // Ensure we're getting a fresh response
            }
        });
        
        const responseTime = Date.now() - startTime;
        console.log(`API warmed: ${endpoint} (${responseTime}ms)`);
        
        return { 
            endpoint, 
            status: 'success',
            responseTime
        };
    } catch (error: any) {
        console.error(`Failed to warm API ${endpoint}:`, error.message);
        return { 
            endpoint, 
            status: 'error', 
            error: error.message,
            responseTime: Date.now() - startTime
        };
    }
}

// Warm all APIs for a single set sequentially
async function warmSetApis(setid: string): Promise<{
    setid: string;
    endpoints: Array<{ 
        endpoint: string; 
        status: string; 
        responseTime?: number;
        error?: string;
    }>;
    revalidation?: {
        status: string;
        time?: number;
        error?: string;
    }
}> {
    const endpoints = getSetApiEndpoints(setid);
    const results: Array<{ 
        endpoint: string; 
        status: string; 
        responseTime?: number;
        error?: string;
    }> = [];
    
    // Warm APIs one by one to avoid overwhelming the backend
    for (const endpoint of endpoints) {
        const result = await warmUpApiEndpoint(endpoint);
        results.push(result);
    }
    
    // After warming all APIs for this set, trigger ISR revalidation
    let revalidationResult: {
        status: string;
        time?: number;
        error?: string;
    } = {
        status: 'not_attempted'
    };
    
    try {
        const revalidateUrl = `https://www.pokefolio.co/api/revalidate?path=/sets/${setid}`;
        const startTime = Date.now();
        
        const response = await axios.get(revalidateUrl, {
            timeout: 10000,
            headers: { 'x-warming-request': 'true' }
        });
        
        const endTime = Date.now();
        revalidationResult = {
            status: 'success',
            time: endTime - startTime
        };
        
        console.log(`Revalidated ISR for /sets/${setid} (${endTime - startTime}ms)`);
    } catch (error: any) {
        console.error(`Failed to revalidate ISR for /sets/${setid}:`, error.message);
        revalidationResult = {
            status: 'error',
            error: error.message
        };
    }
    
    return {
        setid,
        endpoints: results,
        revalidation: revalidationResult
    };
}

// Phase 2: Warm up the page itself
async function warmUpPage(setid: string): Promise<{ 
    setid: string; 
    status: string; 
    responseTime?: number;
    error?: string;
}> {
    const url = `https://www.pokefolio.co/sets/${setid}`;
    const startTime = Date.now();
    
    try {
        await AxiosInstance.get(url, {
            timeout: PAGE_TIMEOUT,
            headers: { 'x-warming-request': 'true' }
        });
        
        const responseTime = Date.now() - startTime;
        console.log(`Page warmed: ${setid} (${responseTime}ms)`);
        
        return { 
            setid, 
            status: 'success',
            responseTime
        };
    } catch (error: any) {
        if (error.response?.status === 504) {
            console.log(`504 timeout for ${url} - page will be cached for next visit`);
            return { 
                setid, 
                status: 'timeout_success',
                responseTime: Date.now() - startTime
            };
        }
        console.error(`Failed to warm up page ${url}:`, error.message);
        return { 
            setid, 
            status: 'error', 
            error: error.message,
            responseTime: Date.now() - startTime
        };
    }
}

// Phase 3: Extract and warm images from the page
async function extractAndWarmImages(setid: string): Promise<{ 
    setid: string; 
    status: string; 
    error?: string;
    imageResults?: Array<{ url: string; status: string; error?: string }>;
}> {
    const url = `https://www.pokefolio.co/sets/${setid}`;
    try {
        const response = await AxiosInstance.get(url);
        const html = response.data as string;
        
        // Extract image URLs
        const imgRegex = /https:\/\/[^"']*\.(?:png|jpg|jpeg|webp|avif)/g;
        const matches = html.match(imgRegex) || [];
        const uniqueUrls = Array.from(new Set(matches)) as string[]; // Remove duplicates
        const filteredUrls = uniqueUrls.filter(url => !url.includes('_hires'));
        const imageUrls = filteredUrls.slice(0, MAX_IMAGES_PER_PAGE);
        
        //console.log(`Found ${imageUrls.length} images for ${url}`);
        
        // Warm image URLs in parallel
        const imageResults = await Promise.all(
            imageUrls.map(async (imageUrl) => {
                try {
                    await axios.get(imageUrl, { 
                        method: 'HEAD',
                        timeout: IMAGE_TIMEOUT
                    });
                    return { url: imageUrl, status: 'success' };
                } catch (error: any) {
                    console.error(`Failed to warm image ${imageUrl}:`, error.message);
                    return { url: imageUrl, status: 'error', error: error.message };
                }
            })
        );

        return { 
            setid, 
            status: 'success',
            imageResults
        };
    } catch (error: any) {
        console.error(`Failed to extract images from ${url}:`, error.message);
        return { setid, status: 'error', error: error.message };
    }
}

// Main warm-up function
export default async function WarmUp(db: any) {
    try {
        // Get sets ordered by priority (could be by release date, popularity, etc.)
        const sets = await db.any(`
            SELECT s.setid 
            FROM pfdata_sets s
            ORDER BY s.releaseddate DESC
        `) as Array<{setid: string}>;
        
        console.log(`Starting three-phase warm-up process for ${sets.length} sets...`);
        
        // Initialize results structure with proper TypeScript types
        const results = {
            apis: [] as Array<{
                setid: string;
                endpoints: Array<{ 
                    endpoint: string; 
                    status: string; 
                    responseTime?: number;
                    error?: string;
                }>;
            }>,
            pages: [] as Array<{ 
                setid: string; 
                status: string; 
                responseTime?: number;
                error?: string;
            }>,
            images: [] as Array<{ 
                setid: string; 
                status: string; 
                error?: string;
                imageResults?: Array<{ url: string; status: string; error?: string }>;
            }>
        };
        
        // Process sets in batches
        for (let i = 0; i < sets.length; i += BATCH_SIZE) {
            const batchSets = sets.slice(i, i + BATCH_SIZE);
            //console.log(`Processing batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(sets.length/BATCH_SIZE)}: sets ${i+1}-${Math.min(i+BATCH_SIZE, sets.length)}`);
            
            // Phase 1: Warm API endpoints for this batch
            //console.log(`Phase 1: Warming API endpoints for batch ${Math.floor(i/BATCH_SIZE) + 1}...`);
            for (const set of batchSets) {
                const apiResult = await warmSetApis(set.setid);
                results.apis.push(apiResult);
            }
            
            // Brief pause between phases
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // Phase 2: Warm pages for this batch (now that APIs should be cached)
            //console.log(`Phase 2: Warming pages for batch ${Math.floor(i/BATCH_SIZE) + 1}...`);
            const pageResults = await Promise.all(
                batchSets.map(set => warmUpPage(set.setid))
            );
            results.pages.push(...pageResults);
            
            // Brief pause between phases
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // Phase 3: Extract and warm images for this batch
            //console.log(`Phase 3: Warming images for batch ${Math.floor(i/BATCH_SIZE) + 1}...`);
            const imageResults = await Promise.all(
                batchSets.map(set => extractAndWarmImages(set.setid))
            );
            results.images.push(...imageResults);
            
            // Delay before next batch
            if (i + BATCH_SIZE < sets.length) {
                //console.log(`Batch complete. Pausing before next batch...`);
                await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
            }
        }

        // Wait before retrying failures
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));

        // Retry failed pages
        const failedPages = results.pages
            .filter(r => r.status === 'error')
            .map(r => r.setid);

        if (failedPages.length > 0) {
            //console.log(`Retrying ${failedPages.length} failed pages...`);
            
            for (const setid of failedPages) {
                const retryResult = await warmUpPage(setid);
                const index = results.pages.findIndex(r => r.setid === setid);
                if (index !== -1) {
                    results.pages[index] = retryResult;
                }
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }

        // Compile statistics
        
        // API Statistics
        const apiEndpointCount = results.apis.reduce((total, set) => total + set.endpoints.length, 0);
        const apiSuccessCount = results.apis.reduce((total, set) => 
            total + set.endpoints.filter(e => e.status === 'success').length, 0);
        const apiFailCount = apiEndpointCount - apiSuccessCount;
        
        let totalApiTime = 0;
        let apiTimeCount = 0;
        results.apis.forEach(set => {
            set.endpoints.forEach(endpoint => {
                if (endpoint.responseTime !== undefined) {
                    totalApiTime += endpoint.responseTime;
                    apiTimeCount++;
                }
            });
        });
        const avgApiTime = apiTimeCount > 0 ? Math.round(totalApiTime / apiTimeCount) : 0;
        
        // Page Statistics
        const pageSuccessCount = results.pages.filter(r => r.status === 'success').length;
        const pageTimeoutCount = results.pages.filter(r => r.status === 'timeout_success').length;
        const pageFailCount = results.pages.filter(r => r.status === 'error').length;
        
        let totalPageTime = 0;
        let pageTimeCount = 0;
        results.pages.forEach(page => {
            if (page.responseTime !== undefined) {
                totalPageTime += page.responseTime;
                pageTimeCount++;
            }
        });
        const avgPageTime = pageTimeCount > 0 ? Math.round(totalPageTime / pageTimeCount) : 0;
        
        // Image Statistics
        const imageStats = {
            total: 0,
            successful: 0,
            failed: 0
        };

        results.images.forEach(result => {
            if (result.imageResults) {
                imageStats.total += result.imageResults.length;
                imageStats.successful += result.imageResults.filter(r => r.status === 'success').length;
                imageStats.failed += result.imageResults.filter(r => r.status === 'error').length;
            }
        });

        // Log comprehensive results
        console.log(`\n==== Warm-up Process Completed ====\n`);
        
        console.log(`API Endpoints:`);
        console.log(`- Total warmed: ${apiEndpointCount}`);
        console.log(`- Successfully warmed: ${apiSuccessCount} (${Math.round(apiSuccessCount/apiEndpointCount*100)}%)`);
        console.log(`- Failed: ${apiFailCount} (${Math.round(apiFailCount/apiEndpointCount*100)}%)`);
        console.log(`- Average response time: ${avgApiTime}ms`);
        
        console.log(`\nPages:`);
        console.log(`- Total warmed: ${sets.length}`);
        console.log(`- Successfully warmed: ${pageSuccessCount} (${Math.round(pageSuccessCount/sets.length*100)}%)`);
        console.log(`- Timeout (cached): ${pageTimeoutCount} (${Math.round(pageTimeoutCount/sets.length*100)}%)`);
        console.log(`- Failed: ${pageFailCount} (${Math.round(pageFailCount/sets.length*100)}%)`);
        console.log(`- Average response time: ${avgPageTime}ms`);
        
        console.log(`\nImages:`);
        console.log(`- Total processed: ${imageStats.total}`);
        console.log(`- Successfully warmed: ${imageStats.successful} (${Math.round(imageStats.successful/imageStats.total*100)}%)`);
        console.log(`- Failed: ${imageStats.failed} (${Math.round(imageStats.failed/imageStats.total*100)}%)`);

        // Return structured results
        return {
            overall: {
                setsProcessed: sets.length,
                completionTime: new Date().toISOString()
            },
            apis: {
                total: apiEndpointCount,
                successful: apiSuccessCount,
                failed: apiFailCount,
                averageResponseTime: avgApiTime
            },
            pages: {
                total: sets.length,
                successful: pageSuccessCount,
                timeoutSuccess: pageTimeoutCount,
                failed: pageFailCount,
                averageResponseTime: avgPageTime
            },
            images: imageStats,
            detailedResults: results
        };

    } catch (error) {
        console.error('Error during warm-up process:', error);
        throw error;
    }
}