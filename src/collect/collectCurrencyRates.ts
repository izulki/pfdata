import axios from 'axios';
import PTCGConfig from '../utils/axios_ptcg_config';
const pgp = require('pg-promise')();

const AxiosInstance = axios.create(PTCGConfig());

interface CollectCurrencyRatesResponse {
    state: boolean,
    errors: number,
    log: number
}

interface CurrencyResponse {
    success: boolean;
    timestamp: number;
    date: string;
    base: string;
    rates: Record<string, number>;
}

export default async function CollectCurrencyRates(db: any, method: string): Promise<CollectCurrencyRatesResponse> {
    let errors = 0;

    try {
        const { data: response } = await AxiosInstance.get<CurrencyResponse>('https://api.fxratesapi.com/latest');

        if (!response.success || response.base !== 'USD') {
            throw new Error('Invalid response or base currency is not USD');
        }

        // Only include 3-character currency codes
        const values = Object.entries(response.rates)
            .filter(([currency]) => currency.length === 3)
            .map(([currency, rate]) =>
                `('${currency}', ${rate}, '${new Date(response.date).toISOString()}')`
            )
            .join(',');

        const query = `
            INSERT INTO usd_pairs (currency, rate, timestamp)
            VALUES ${values}
            ON CONFLICT (currency) 
            DO UPDATE SET
                rate = EXCLUDED.rate,
                timestamp = EXCLUDED.timestamp
        `;

        await db.query(query);
        console.log(`Successfully updated currency rates at ${new Date().toISOString()}`);
    } catch (e) {
        console.log("Currency Rate Error", e);
        errors++;
    }

    return {
        state: true,
        errors: errors,
        log: 0
    };
}