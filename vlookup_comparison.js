/**
 * BOCRA vs. BTC Subscriber Comparison Tool
 * 
 * This script compares BOCRA verified subscriber data with BTC internal subscriber data
 * to identify subscribers present in one system but not the other.
 * 
 * Features:
 * - Reads BOCRA data from resources folder (verified subscriber CSVs)
 * - Reads BTC internal data from Vlookup folder
 * - Performs comparison based on MSISDN (phone number)
 * - Generates reports for missing/extra subscribers in each system
 * - Handles large datasets efficiently with streaming
 */

import fs from 'fs/promises';
import fs_regular from 'fs';
import path from 'path';
import { createReadStream, createWriteStream } from 'fs';
import { pipeline } from 'stream/promises';
import { parse as csvParse } from 'csv-parse';
import { stringify as csvStringify } from 'csv-stringify';
import { Transform } from 'stream';
import AdmZip from 'adm-zip';
import { fileURLToPath } from 'url';

// Configuration
const BOCRA_DATA_DIR = './resources';  // Directory containing BOCRA verified subscriber data
const BTC_DATA_DIR = './Vlookup';      // Directory containing BTC subscriber data
const OUTPUT_DIR = './comparison_results'; // Directory for output reports
const PREPAID_ZIP_FILE = path.join(BTC_DATA_DIR, 'prepaid_mobile_numbers_20250501002001.zip');
const POSTPAID_CSV_FILE = path.join(BTC_DATA_DIR, 'postpaid_mobile_numbers_20250501000001 2.csv');

// Create output directory if it doesn't exist
async function ensureOutputDir() {
    try {
        await fs.mkdir(OUTPUT_DIR, { recursive: true });
        console.log(`Created output directory: ${OUTPUT_DIR}`);
    } catch (err) {
        console.error('Error creating output directory:', err);
        throw err;
    }
}

// Function to find all BOCRA verified subscriber files
async function findBocraVerifiedFiles() {
    try {
        const files = await fs.readdir(BOCRA_DATA_DIR);
        const verifiedFiles = files.filter(file => 
            file.startsWith('verified_natural-subscriber') && file.endsWith('.csv')
        );
        
        if (verifiedFiles.length === 0) {
            throw new Error('No BOCRA verified subscriber files found');
        }
        
        console.log(`Found ${verifiedFiles.length} BOCRA verified subscriber files`);
        return verifiedFiles.map(file => path.join(BOCRA_DATA_DIR, file));
    } catch (err) {
        console.error('Error finding BOCRA verified files:', err);
        throw err;
    }
}

// Function to extract MSISDNs from BOCRA verified subscriber files
async function extractBocraMsisdns(files) {
    console.log('Extracting MSISDNs from BOCRA verified files...');
    const msisdnSet = new Set();
    
    for (const file of files) {
        try {
            console.log(`Processing BOCRA file: ${file}`);
            
            // Create a read stream and parser
            const parser = csvParse({
                columns: true,
                skip_empty_lines: true,
                trim: true
            });
            
            // Process each row
            parser.on('readable', function() {
                let record;
                while ((record = parser.read()) !== null) {
                    if (record.msisdn) {
                        // Clean the MSISDN (remove any non-digit characters and ensure format)
                        const msisdn = record.msisdn.replace(/\D/g, '');
                        
                        // Only add valid MSISDNs (assuming Botswana numbers starting with 7)
                        if (msisdn.length === 8 && msisdn.startsWith('7')) {
                            msisdnSet.add(msisdn);
                        }
                    }
                }
            });
            
            // Set up the pipeline
            await pipeline(
                createReadStream(file),
                parser
            );
            
            console.log(`Processed ${file}, total unique MSISDNs: ${msisdnSet.size}`);
        } catch (err) {
            console.error(`Error processing BOCRA file ${file}:`, err);
            // Continue with the next file instead of failing completely
        }
    }
    
    console.log(`Completed extraction. Total unique BOCRA MSISDNs: ${msisdnSet.size}`);
    return msisdnSet;
}

// Function to extract MSISDNs from BTC prepaid subscribers (ZIP file)
async function extractBtcPrepaidMsisdns() {
    console.log(`Extracting BTC prepaid MSISDNs from: ${PREPAID_ZIP_FILE}`);
    const msisdnSet = new Set();
    
    try {
        // Check if ZIP file exists
        if (!fs_regular.existsSync(PREPAID_ZIP_FILE)) {
            console.warn(`Prepaid ZIP file not found: ${PREPAID_ZIP_FILE}`);
            return msisdnSet;
        }
        
        // Read and extract the ZIP file
        const zip = new AdmZip(PREPAID_ZIP_FILE);
        const zipEntries = zip.getEntries();
        
        // Process each CSV file in the ZIP
        for (const entry of zipEntries) {
            if (entry.entryName.endsWith('.csv')) {
                console.log(`Processing ZIP entry: ${entry.entryName}`);
                
                // Extract the file to a temporary location
                const tempFile = path.join(OUTPUT_DIR, `temp_${Date.now()}.csv`);
                zip.extractEntryTo(entry, OUTPUT_DIR, false, true, false, `temp_${Date.now()}.csv`);
                
                // Create a read stream and parser
                const parser = csvParse({
                    columns: true,
                    skip_empty_lines: true,
                    trim: true
                });
                
                // Process each row
                parser.on('readable', function() {
                    let record;
                    while ((record = parser.read()) !== null) {
                        // The field name might vary, try common possibilities
                        const msisdn = record.msisdn || record.MSISDN || record.mobile_number || record.PhoneNumber;
                        
                        if (msisdn) {
                            // Clean the MSISDN (remove any non-digit characters and ensure format)
                            const cleanMsisdn = msisdn.toString().replace(/\D/g, '');
                            
                            // Only add valid MSISDNs (assuming Botswana numbers starting with 7)
                            if (cleanMsisdn.length === 8 && cleanMsisdn.startsWith('7')) {
                                msisdnSet.add(cleanMsisdn);
                            }
                        }
                    }
                });
                
                // Set up the pipeline
                await pipeline(
                    createReadStream(tempFile),
                    parser
                );
                
                // Clean up temporary file
                await fs.unlink(tempFile);
                console.log(`Processed ${entry.entryName}, current unique MSISDNs: ${msisdnSet.size}`);
            }
        }
    } catch (err) {
        console.error('Error processing BTC prepaid data:', err);
        // Continue instead of failing completely
    }
    
    console.log(`Completed extraction. Total unique BTC prepaid MSISDNs: ${msisdnSet.size}`);
    return msisdnSet;
}

// Function to extract MSISDNs from BTC postpaid subscribers
async function extractBtcPostpaidMsisdns() {
    console.log(`Extracting BTC postpaid MSISDNs from: ${POSTPAID_CSV_FILE}`);
    const msisdnSet = new Set();
    
    try {
        // Check if CSV file exists
        if (!fs_regular.existsSync(POSTPAID_CSV_FILE)) {
            console.warn(`Postpaid CSV file not found: ${POSTPAID_CSV_FILE}`);
            return msisdnSet;
        }
        
        // Create a read stream and parser
        const parser = csvParse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        });
        
        // Process each row
        parser.on('readable', function() {
            let record;
            while ((record = parser.read()) !== null) {
                // The field name might vary, try common possibilities
                const msisdn = record.msisdn || record.MSISDN || record.mobile_number || record.PhoneNumber;
                
                if (msisdn) {
                    // Clean the MSISDN (remove any non-digit characters and ensure format)
                    const cleanMsisdn = msisdn.toString().replace(/\D/g, '');
                    
                    // Only add valid MSISDNs (assuming Botswana numbers starting with 7)
                    if (cleanMsisdn.length === 8 && cleanMsisdn.startsWith('7')) {
                        msisdnSet.add(cleanMsisdn);
                    }
                }
            }
        });
        
        // Set up the pipeline
        await pipeline(
            createReadStream(POSTPAID_CSV_FILE),
            parser
        );
        
        console.log(`Completed postpaid extraction. Total unique BTC postpaid MSISDNs: ${msisdnSet.size}`);
    } catch (err) {
        console.error('Error processing BTC postpaid data:', err);
        // Continue instead of failing completely
    }
    
    return msisdnSet;
}

// Function to write MSISDNs to a CSV file
async function writeMsisdnsToFile(msisdns, filename) {
    console.log(`Writing ${msisdns.size} MSISDNs to file: ${filename}`);
    
    const outputStream = createWriteStream(filename);
    const stringifier = csvStringify({
        header: true,
        columns: ['msisdn']
    });
    
    for (const msisdn of msisdns) {
        stringifier.write({ msisdn });
    }
    
    stringifier.end();
    await pipeline(stringifier, outputStream);
    console.log(`Completed writing to ${filename}`);
}

// Main function to perform the comparison
async function compareSubscriberData() {
    try {
        console.log('Starting BOCRA vs. BTC subscriber comparison...');
        
        // Ensure output directory exists
        await ensureOutputDir();
        
        // Find BOCRA verified files
        const bocraFiles = await findBocraVerifiedFiles();
        
        // Extract MSISDNs from all data sources
        const bocraMsisdns = await extractBocraMsisdns(bocraFiles);
        const btcPrepaidMsisdns = await extractBtcPrepaidMsisdns();
        const btcPostpaidMsisdns = await extractBtcPostpaidMsisdns();
        
        // Combine BTC prepaid and postpaid MSISDNs
        const btcMsisdns = new Set([...btcPrepaidMsisdns, ...btcPostpaidMsisdns]);
        console.log(`Total unique BTC MSISDNs (prepaid + postpaid): ${btcMsisdns.size}`);
        
        // Determine MSISDNs in BOCRA but not in BTC
        const onlyInBocra = new Set(
            [...bocraMsisdns].filter(msisdn => !btcMsisdns.has(msisdn))
        );
        console.log(`MSISDNs in BOCRA but not in BTC: ${onlyInBocra.size}`);
        
        // Determine MSISDNs in BTC but not in BOCRA
        const onlyInBtc = new Set(
            [...btcMsisdns].filter(msisdn => !bocraMsisdns.has(msisdn))
        );
        console.log(`MSISDNs in BTC but not in BOCRA: ${onlyInBtc.size}`);
        
        // Generate timestamp for filenames
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        
        // Write results to files
        await writeMsisdnsToFile(
            onlyInBocra, 
            path.join(OUTPUT_DIR, `bocra_only_subscribers_${timestamp}.csv`)
        );
        
        await writeMsisdnsToFile(
            onlyInBtc, 
            path.join(OUTPUT_DIR, `btc_only_subscribers_${timestamp}.csv`)
        );
        
        // Generate summary report
        const summaryPath = path.join(OUTPUT_DIR, `comparison_summary_${timestamp}.txt`);
        const summaryContent = [
            'BOCRA vs. BTC Subscriber Comparison Summary',
            '===========================================',
            `Generated: ${new Date().toLocaleString()}`,
            '',
            `Total BOCRA verified subscribers: ${bocraMsisdns.size}`,
            `Total BTC subscribers: ${btcMsisdns.size}`,
            `  - Prepaid subscribers: ${btcPrepaidMsisdns.size}`,
            `  - Postpaid subscribers: ${btcPostpaidMsisdns.size}`,
            '',
            `Subscribers in BOCRA but not in BTC: ${onlyInBocra.size}`,
            `Subscribers in BTC but not in BOCRA: ${onlyInBtc.size}`,
            '',
            'Files generated:',
            `  - ${path.join(OUTPUT_DIR, `bocra_only_subscribers_${timestamp}.csv`)}`,
            `  - ${path.join(OUTPUT_DIR, `btc_only_subscribers_${timestamp}.csv`)}`,
            '',
            'End of report'
        ].join('\n');
        
        await fs.writeFile(summaryPath, summaryContent, 'utf8');
        console.log(`Summary report written to: ${summaryPath}`);
        
        console.log('Comparison completed successfully');
        return {
            bocraSubscriberCount: bocraMsisdns.size,
            btcSubscriberCount: btcMsisdns.size,
            onlyInBocraCount: onlyInBocra.size,
            onlyInBtcCount: onlyInBtc.size,
            summaryPath,
            timestamp
        };
        
    } catch (err) {
        console.error('Error during comparison:', err);
        throw err;
    }
}

// Execute the comparison if this file is run directly
if (import.meta.url && process.argv[1] === fileURLToPath(import.meta.url)) {
    compareSubscriberData()
        .then(result => {
            console.log('Comparison results:', result);
            process.exit(0);
        })
        .catch(err => {
            console.error('Fatal error during comparison:', err);
            process.exit(1);
        });
}

// Export functions for use in other scripts
export { 
    compareSubscriberData,
    extractBocraMsisdns,
    extractBtcPrepaidMsisdns,
    extractBtcPostpaidMsisdns,
    writeMsisdnsToFile
}; 