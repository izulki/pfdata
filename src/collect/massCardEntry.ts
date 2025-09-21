import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import axios from 'axios';

// Configuration
const APIURL = process.env.APIURL || 'https://api.pokefolio.co/api/v2';
const CSV_FILE_PATH = './massentry/mass-card-entry-20250906-final.csv';

// Types
interface CSVRow {
  'Card ID': string;
  'Name': string;
  'Card Number': string;
  'Type': string;
  'hp': string;
  'Rarity': string;
  'Artist': string;
  '': number; // Column H (empty header)
  'Set Name': string;
  'Set ID': string;
  'Image': string;
  'TCCGP ID': number;
  'TCGP Variant': string;
  'PF Variant': string;
}

interface CardEntryPayload {
  cardid: string;
  name: string;
  number: string;
  supertype: string;
  hp: string;
  rarity: string;
  artist: string;
  setid: string;
  images: {
    small: string;
    large: string;
  };
}

interface VariantEntryPayload {
  cardid: string;
  tcgp_id: number;
  tcgp_variant: string;
  pf_variant: string;
}

// Helper function to add delay between requests
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Function to read CSV file
async function readCSV(): Promise<CSVRow[]> {
  return new Promise((resolve, reject) => {
    const results: CSVRow[] = [];
    
    if (!fs.existsSync(CSV_FILE_PATH)) {
      reject(new Error(`CSV file not found: ${CSV_FILE_PATH}`));
      return;
    }

    fs.createReadStream(CSV_FILE_PATH)
      .pipe(csv())
      .on('data', (data: CSVRow) => {
        results.push(data);
      })
      .on('end', () => {
        console.log(`Successfully read ${results.length} rows from CSV`);
        resolve(results);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

// Function to create card entry
async function createCardEntry(row: CSVRow): Promise<boolean> {
  try {
    const payload: CardEntryPayload = {
      cardid: row['Card ID'],
      name: row['Name'],
      number: row['Card Number'],
      supertype: row['Type'],
      hp: row['HP'], // Default to 0 if HP is not provided
      rarity: row['Rarity'],
      artist: row['Artist'],
      setid: row['Set ID'],
      images: {
        small: row['Image'],
        large: row['Image']
      }
    };

    console.log(`Creating card entry for: ${payload.name} (${payload.cardid})`);
    
    const response = await axios.post(`${APIURL}/admin/entry/card`, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.POKEFOLIO_ADMIN_API_BEARER}`,
      }
    });

    console.log(`✅ Card entry created successfully: ${payload.cardid}`);
    return true;
  } catch (error: any) {
    console.error(`❌ Failed to create card entry for ${row['Card ID']}:`, error);
    if (axios.isAxiosError(error)) {
      console.error('Response data:', error.response?.data);
      console.error('Status:', error.response?.status);
    }
    return false;
  }
}

// Function to add variants
async function addVariants(row: CSVRow): Promise<boolean> {
  try {
    const payload: VariantEntryPayload = {
      cardid: row['Card ID'],
      tcgp_id: row['TCCGP ID'],
      tcgp_variant: row['TCGP Variant'],
      pf_variant: row['PF Variant']
    };

    console.log(`Adding variants for card: ${payload.cardid}`);
    
    const response = await axios.post(`${APIURL}/admin/entry/variants`, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.POKEFOLIO_ADMIN_API_BEARER}`,
      }
    });

    console.log(`✅ Variants added successfully: ${payload.cardid}`);
    return true;
  } catch (error: any) {
    console.error(`❌ Failed to add variants for ${row['Card ID']}:`, error);
    if (axios.isAxiosError(error)) {
      console.error('Response data:', error.response?.data);
      console.error('Status:', error.response?.status);
    }
    return false;
  }
}

// Main function to process all entries
export default async function massCardEntry() {
  try {
    console.log('Starting mass entry process...');
    
    // Read CSV file
    const rows = await readCSV();
    
    if (rows.length === 0) {
      console.log('No data found in CSV file');
      return;
    }

    let successfulCards = 0;
    let successfulVariants = 0;
    let failedCards = 0;
    let failedVariants = 0;

    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      console.log(`\nProcessing row ${i + 1}/${rows.length}`);

      // Create card entry
      const cardSuccess = await createCardEntry(row);
      if (cardSuccess) {
        successfulCards++;
      } else {
        failedCards++;
      }
      
      // Add a small delay before adding variants
      await delay(500);
      
      // Always try to add variants, regardless of card creation success
      const variantSuccess = await addVariants(row);
      if (variantSuccess) {
        successfulVariants++;
      } else {
        failedVariants++;
      }

      // Add delay between entries to avoid rate limiting
      if (i < rows.length - 1) {
        await delay(1000);
      }
    }

    // Summary
    console.log('\n🎉 Mass entry process completed!');
    console.log(`📊 Summary:`);
    console.log(`   Cards: ${successfulCards} successful, ${failedCards} failed`);
    console.log(`   Variants: ${successfulVariants} successful, ${failedVariants} failed`);
    console.log(`   Total rows processed: ${rows.length}`);

  } catch (error) {
    console.error('❌ Error in mass entry process:', error);
  }
}

// Error handling for unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Run the script
if (require.main === module) {
  massCardEntry();
}

export { massCardEntry, readCSV, createCardEntry, addVariants };