/**
 * BOCRA SIMS API Integration Server
 * This service fetches subscriber data from BOCRA's SIMS API and uploads it to a BDAP server.
 * The system runs on a weekly schedule and handles both natural and juristic subscribers.
 */

import { exec } from 'child_process';
import path from 'path';
import fs from 'fs/promises';
import axios from 'axios';
import cron from 'node-cron';
import dotenv from 'dotenv';
import os from 'os';
import https from 'https';

// Add fs regular import for the natural subscriber implementation
import fs_regular from 'fs';

console.log('Starting application...');

// Load environment variables from .env file
dotenv.config();
console.log('Environment variables loaded');

// Core configuration constants
const OUTPUT_DIR = './output';  // Local directory for storing CSV files
const BASE_URL = 'https://simsapi.bocra.org.bw';  // API base URL
const API_KEY = 'BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA=';  // API authentication key
const SSH_KEY_PATH = path.join(os.homedir(), '.ssh', 'id_rsa');  // Path to SSH key for BDAP server authentication
const NATURAL_API_URL = 'https://simsapi.bocra.org.bw/api/v1/natural_subscribers/mno/verification';  // Endpoint for natural subscribers

// Constants for natural subscriber implementation
const DEFAULT_CONTACT = "+26773001762";
const NATURAL_BASE_URL = 'https://simsapi.bocra.org.bw/api/v1/natural_subscribers/mno/verification';
const NATURAL_OUTPUT_DIR = "resources/";
const DATE_RANGE = '/start/01-01-2022/end/01-01-2026';

// Log SSH key path for debugging
console.log('Using SSH key:', SSH_KEY_PATH);

/**
 * BDAP Server Configuration - loaded from environment variables
 * Used for secure file transfer via SCP to the BDAP server
 */
const BDAP_CONFIG = {
    host: process.env.BDAP_HOST,          // BDAP server hostname
    port: parseInt(process.env.BDAP_PORT) || 22,  // SSH port (default: 22)
    username: process.env.BDAP_USERNAME,  // SSH username
    password: process.env.BDAP_PASSWORD,  // SSH password
    remoteDir: process.env.BDAP_REMOTE_DIR  // Remote directory for file storage
};

/**
 * Validates that all required BDAP configuration parameters are present
 * Throws an error if any required parameter is missing
 */
function validateBDAPConfig() {
    const required = ['host', 'username', 'password', 'remoteDir'];
    const missing = required.filter(key => !BDAP_CONFIG[key]);
    if (missing.length > 0) {
        throw new Error(`Missing required BDAP configuration: ${missing.join(', ')}`);
    }
}

// Test data for API connectivity checks
const TEST_DOCUMENT = '209126618';  // Test document number for verification
const TEST_MSISDN = '73005659';     // Test MSISDN (phone number) for lookups

// Data collection configuration
const HISTORICAL_START_DATE = '2022-01-01';  // Starting date for historical data collection
const HISTORICAL_END_DATE = new Date().toISOString().split('T')[0]; // Current date as end date
const PAGE_SIZE = 20;    // Reduced from 2000 to 20 to match Python implementation
const MAX_PAGES = 3;     // Maximum number of pages for testing
const MAX_PAGES_FULL = 1000; // Maximum number of pages for production (effectively unlimited)
const DISABLE_DATE_FILTERING = false; // Changed back to false - we'll use date filtering with correct format

// MSISDN range and retry configuration
const MSISDN_START = 70000000; // Starting MSISDN range for Botswana
const MSISDN_END = 79999999;   // Ending MSISDN range for Botswana
const BATCH_SIZE = 100;        // Number of MSISDNs to process in parallel
const MAX_RETRIES = 3;         // Maximum number of retry attempts
const RETRY_DELAY = 5000;      // Delay between retries (5 seconds)
const CONNECTION_TIMEOUT = 30000; // Connection timeout (30 seconds)

// Create a dedicated API client for natural subscribers based on exact implementation from user
const naturalApiClient = axios.create({
    headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
    },
    timeout: 30000,
});

// Functions for natural subscriber processing exactly as provided
async function writeToFileNatural(filePath, content) {
    // Ensure output directory exists
    if (!fs_regular.existsSync(NATURAL_OUTPUT_DIR)) {
        fs_regular.mkdirSync(NATURAL_OUTPUT_DIR);
    }
    await fs_regular.promises.appendFile(filePath, content + "\n", "utf-8");
}

async function withRetryNatural(fn, retries = 3) {
    try {
        return await fn();
    } catch (error) {
        if (retries > 0) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            return withRetryNatural(fn, retries - 1);
        }
        throw error;
    }
}

async function safeSendNotificationNatural(message, contact) {
    try {
        // await sendNotification(message, contact);
        console.log(`Sending ${message} to ${contact}`);
    } catch (error) {
        console.error('Failed to send notification:', error);
    }
}

// Natural subscriber download implementation exactly as provided
async function downloadNaturalSubscribers(config) {
    const { status, fileNamePrefix, notificationContact = DEFAULT_CONTACT } = config;
    const contact = notificationContact;
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `${fileNamePrefix}_${timestamp}.csv`;
    const filePath = path.join(NATURAL_OUTPUT_DIR, filename);

    try {
        await safeSendNotificationNatural(`Starting download for ${status} subscribers`, contact);

        // Initialize CSV file
        const initialRow = 'firstname,lastname,dateOfBirth,country,documentType,documentNumber,expiryDate,dateOfIssue,sex,state,updated,msisdn,naturalSubscriber,juristicSubscriber,registrationNumber,verificationReason,verificationState';
        await writeToFileNatural(filePath, initialRow)

        // Fetch initial data to get pagination details
        const initialResponse = await withRetryNatural(() =>
            naturalApiClient.get(`${NATURAL_BASE_URL}/${status}${DATE_RANGE}?size=2000`)
        );

        // Use actual total pages from API response instead of hardcoded value
        const totalPages = initialResponse.data.pages || 1;
        console.log(`Processing ${totalPages} pages of ${status} subscribers (FULL DATA)`);

        // Process all pages
        for (let currentPage = 0; currentPage < totalPages; currentPage++) {
            try {
                console.log(`Processing page ${currentPage + 1}/${totalPages}`);
                const pageData = await withRetryNatural(() =>
                    naturalApiClient.get(`${NATURAL_BASE_URL}/${status}${DATE_RANGE}?size=2000&page=${currentPage}`)
                );

                if (!pageData.data.content?.length) {
                    console.log(`No content found in page ${currentPage}`);
                    continue;
                }

                const records = pageData.data.content.flatMap(subscriber =>
                    subscriber.numbers.map(number => ({
                        firstname: subscriber.firstName,
                        lastname: subscriber.lastName,
                        dateOfBirth: subscriber.dateOfBirth,
                        country: subscriber.country,
                        documentType: subscriber.subscriberDocuments[0]?.documentType || '',
                        documentNumber: subscriber.subscriberDocuments[0]?.documentNumber || '',
                        expiryDate: subscriber.subscriberDocuments[0]?.expiryDate || '',
                        dateOfIssue: subscriber.subscriberDocuments[0]?.dateOfIssue || '',
                        sex: subscriber.sex,
                        state: number.state,
                        updated: subscriber.updated,
                        msisdn: number.msisdn,
                        naturalSubscriber: number.naturalSubscriber,
                        juristicSubscriber: number.juristicSubscriber,
                        registrationNumber: number.registrationNumber,
                        verificationReason: subscriber.verificationReason,
                        verificationState: subscriber.verificationState
                    }))
                );

                records.map((record) => {
                    const row = `${record.firstname},${record.lastname}, ${record.dateOfBirth}, ${record.country}, ${record.documentType}, ${record.documentNumber}, ${record.expiryDate},${record.dateOfIssue}, ${record.sex}, ${record.state}, ${record.updated},${record.msisdn},${record.naturalSubscriber},${record.juristicSubscriber},${record.registrationNumber},${record.verificationReason},${record.verificationState}`;
                    writeToFileNatural(filePath, row);
                })

            } catch (pageError) {
                console.error(`Error processing page ${currentPage}:`, pageError);
                await safeSendNotificationNatural(`Error processing page ${currentPage} for ${status} subscribers`, contact);
            }
        }

        await safeSendNotificationNatural(`Successfully downloaded ${status} subscribers`, contact);
        console.log(`Completed download for ${status} subscribers at: ${filePath}`);
        return filePath;

    } catch (error) {
        console.error(`Critical error processing ${status} subscribers:`, error);
        await safeSendNotificationNatural(`Failed to download ${status} subscribers - contact support`, contact);
        throw error;
    } finally {
        // if (fileHandle) {
        //     await fileHandle.close();
        // }
    }
}

// Export functions exactly as provided
export const downloadAllFailedNaturalSubscribers = async (notificationContact) => {
    return downloadNaturalSubscribers({
        status: 'FAILED_VERIFICATION',
        fileNamePrefix: 'failed_natural-subscriber',
        notificationContact
    });
}

export const downloadAllVerifiedNaturalSubscribers = async (notificationContact) => {
    return downloadNaturalSubscribers({
        status: 'VERIFIED',
        fileNamePrefix: 'verified_natural-subscriber',
        notificationContact
    });
}

export const downloadAllPendingNaturalSubscribers = async (notificationContact) => {
    return downloadNaturalSubscribers({
        status: 'PENDING_VERIFICATION',
        fileNamePrefix: 'pending_natural-subscriber',
        notificationContact
    });
}

/**
 * Configure Axios instance for API requests
 * Sets up base URL, timeout, and authentication headers
 */
const api = axios.create({
    baseURL: BASE_URL,
    timeout: 120000,
    headers: {
        'x-api-key': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    },
    // Disable SSL verification to match Python implementation
    httpsAgent: new https.Agent({
        rejectUnauthorized: false
    })
});

/**
 * Request interceptor for debugging API requests
 * Logs request URL, headers, and data before sending
 */
api.interceptors.request.use(request => {
    console.log('Request URL:', request.url);
    console.log('Request Headers:', JSON.stringify(request.headers, null, 2));
    console.log('Request Data:', JSON.stringify(request.data, null, 2));
    return request;
});

/**
 * Response interceptor for debugging API responses
 * Logs response status, headers, and data when received
 * Also handles and logs error responses
 */
api.interceptors.response.use(
    response => {
        console.log('Response Status:', response.status);
        console.log('Response Headers:', JSON.stringify(response.headers, null, 2));
        if (response.data) {
            console.log('Response Data:', JSON.stringify(response.data, null, 2));
        } else {
            console.log('Response Data: Empty');
        }
        return response;
    },
    error => {
        console.log('Error Response:', {
            status: error.response?.status,
            statusText: error.response?.statusText,
            headers: error.response?.headers,
            data: error.response?.data
        });
        return Promise.reject(error);
    }
);

/**
 * Creates a date-partitioned directory path for file storage
 * Structure: ./output/YYYY/MM/DD/
 * @param {Date} date - The date to use for partitioning (default: current date)
 * @returns {string} The full path to the partition directory
 */
async function getDatePartitionedPath(date = new Date()) {
    // Extract year, month, and day from the date
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    
    // Create the partition path and ensure it exists
    const partitionPath = path.join(OUTPUT_DIR, year.toString(), month, day);
    await fs.mkdir(partitionPath, { recursive: true });
    return partitionPath;
}

/**
 * Tests connection to the BDAP server using SSH
 * Verifies credentials and network connectivity
 * @returns {boolean} True if connection successful, false otherwise
 */
async function testBDAPConnection() {
    console.log('Testing BDAP server connection...');
    
    // Construct SSH command to test connection
    const testCmd = `sshpass -p "${BDAP_CONFIG.password}" ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no -p ${BDAP_CONFIG.port} ${BDAP_CONFIG.username}@${BDAP_CONFIG.host} "echo 'Connection test successful'"`;
    
    try {
        // Execute the command and wait for result
        await new Promise((resolve, reject) => {
            exec(testCmd, (error, stdout, stderr) => {
                if (error) {
                    console.error('Connection test failed:', stderr);
                    reject(error);
                } else {
                    console.log('Connection test successful');
                    resolve(stdout);
                }
            });
        });
        return true;
    } catch (error) {
        console.error('Failed to connect to BDAP server:', error.message);
        return false;
    }
}

/**
 * Utility function to introduce a delay
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise} Promise that resolves after the specified time
 */
async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Uploads a file to the BDAP server using SCP
 * Includes connection testing, retry mechanism, and verification
 * @param {string} localFilePath - Path to the local file to upload
 * @returns {boolean} True if upload successful, false otherwise
 */
async function uploadToBDAP(localFilePath) {
    const fileName = path.basename(localFilePath);
    const remoteLocation = `${BDAP_CONFIG.username}@${BDAP_CONFIG.host}:${BDAP_CONFIG.remoteDir}/${fileName}`;
    
    console.log(`Starting SCP transfer for file: ${fileName}`);
    console.log(`Remote location: ${remoteLocation}`);
    
    // Test connection before attempting transfer
    const isConnected = await testBDAPConnection();
    if (!isConnected) {
        throw new Error('Cannot establish connection to BDAP server. Please check your network connection and VPN status.');
    }
    
    // Implement retry mechanism
    let retries = MAX_RETRIES;
    while (retries > 0) {
        try {
            // Step 1: Create remote directory if it doesn't exist
            const mkdirCmd = `sshpass -p "${BDAP_CONFIG.password}" ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no -p ${BDAP_CONFIG.port} ${BDAP_CONFIG.username}@${BDAP_CONFIG.host} "mkdir -p ${BDAP_CONFIG.remoteDir}"`;
            await new Promise((resolve, reject) => {
                exec(mkdirCmd, { timeout: CONNECTION_TIMEOUT }, (error, stdout, stderr) => {
                    if (error && !stderr.includes('File exists')) {
                        console.error('Error creating remote directory:', stderr);
                        reject(error);
                    }
                    resolve(stdout);
                });
            });

            // Step 2: Perform SCP transfer with progress monitoring
            const scpCmd = `sshpass -p "${BDAP_CONFIG.password}" scp -v -o ConnectTimeout=30 -o StrictHostKeyChecking=no -P ${BDAP_CONFIG.port} "${localFilePath}" "${remoteLocation}"`;
            await new Promise((resolve, reject) => {
                const process = exec(scpCmd, { timeout: CONNECTION_TIMEOUT }, (error, stdout, stderr) => {
                    if (error) {
                        console.error('SCP transfer failed:', stderr);
                        reject(error);
                    }
                    resolve(stdout);
                });

                // Monitor transfer progress
                process.stderr.on('data', (data) => {
                    if (data.includes('Sending file modes')) {
                        console.log('Starting file transfer...');
                    } else if (data.includes('Bytes per second')) {
                        console.log('Transfer progress:', data.trim());
                    }
                });
            });

            // Step 3: Verify file transfer by checking the remote file
            const verifyCmd = `sshpass -p "${BDAP_CONFIG.password}" ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no -p ${BDAP_CONFIG.port} ${BDAP_CONFIG.username}@${BDAP_CONFIG.host} "ls -l '${BDAP_CONFIG.remoteDir}/${fileName}'"`;
            const remoteFileInfo = await new Promise((resolve, reject) => {
                exec(verifyCmd, { timeout: CONNECTION_TIMEOUT }, (error, stdout, stderr) => {
                    if (error) {
                        console.error('File verification failed:', stderr);
                        reject(error);
                    }
                    resolve(stdout);
                });
            });

            console.log(`Successfully uploaded ${fileName} to BDAP server`);
            console.log('Remote file info:', remoteFileInfo);
            return true;
            
        } catch (error) {
            // Handle retry logic
            retries--;
            if (retries > 0) {
                console.log(`Upload failed, retrying in ${RETRY_DELAY/1000} seconds... (${retries} retries left)`);
                await sleep(RETRY_DELAY);
            } else {
                console.error('BDAP upload failed after all retries:', error);
                throw new Error(`Failed to upload to BDAP server after ${MAX_RETRIES} attempts: ${error.message}`);
            }
        }
    }
}

/**
 * Fetches subscribers from the BOCRA SIMS API
 * Handles different subscriber types (natural/juristic) and verification states
 * @param {string|null} verificationState - Verification state to filter by (ACTIVE, FAILED, PENDING, or null for all)
 * @param {string} type - Subscriber type ('natural' or 'juristic')
 * @param {string} fromDate - Start date for historical data in YYYY-MM-DD format
 * @returns {Set<string>} Set of JSON-stringified subscriber records
 */
async function fetchAllSubscribers(verificationState = null, type = 'natural', fromDate = HISTORICAL_START_DATE) {
    const results = new Set();
    console.log(`Fetching ${type} subscribers using verification endpoint...`);
    console.log(`Verification state: ${verificationState}`);
    
    try {
        // NATURAL SUBSCRIBERS HANDLING
        if (type === 'natural') {
            console.log(`Using correct natural subscribers endpoint structure...`);
            
            // Map our internal verification states to API-expected values
            const stateMap = {
                'ACTIVE': 'VERIFIED',
                'FAILED': 'FAILED_VERIFICATION',
                'PENDING': 'PENDING_VERIFICATION'
            };
            
            // Determine which states to process
            const statesToProcess = verificationState 
                ? [stateMap[verificationState] || verificationState] 
                : Object.values(stateMap);
            
            console.log(`Will process these verification states: ${statesToProcess.join(', ')}`);
            
            // Format dates for API - YYYY-MM-DD to MM-DD-YYYY (exactly as in TypeScript code)
            const formatDateForApi = (dateStr) => {
                const [year, month, day] = dateStr.split('-');
                return `${month}-${day}-${year}`;
            };
            
            const startDate = formatDateForApi(HISTORICAL_START_DATE);
            const endDate = formatDateForApi(HISTORICAL_END_DATE);
            
            // Use exact date range format from Python code
            const dateRange = `/start/${startDate}/end/${endDate}`;
            
            console.log(`Using Python-matched date range: ${dateRange}`);
            console.log(`Date filtering disabled: ${DISABLE_DATE_FILTERING}`);
            
            // Add variable to track if any state processed successfully
            let anyStateSuccessful = false;
            
            // Process each verification state
            for (const state of statesToProcess) {
                console.log("\n========== Processing " + state + " natural subscribers ==========");
                
                // Try alternative endpoints if main endpoint fails with Python's exact URL structure
                let endpointsToTry = [];
                
                if (DISABLE_DATE_FILTERING) {
                    endpointsToTry = [
                        `/api/v1/natural_subscribers/mno/verification/${state}`,
                        `/api/v1/natural_subscribers/${state}`,
                        `/api/v1/natural_subscribers/verification/${state}`
                    ];
                } else {
                    // Primary endpoint exactly matching Python's approach (status before date range)
                    endpointsToTry = [
                        `/api/v1/natural_subscribers/mno/verification/${state}${dateRange}`
                    ];
                }
                
                let endpointSuccess = false;
                
                for (const fullUrl of endpointsToTry) {
                    if (endpointSuccess) break;
                    
                    console.log(`Trying Python-matched endpoint: ${fullUrl}`);
                    
                    try {
                        // Initialize pagination variables
                        let currentPage = 0;
                        const pageSize = PAGE_SIZE; // Using smaller pageSize (20) to match Python
                        let totalPages = 1;
                        let pagesFetched = 0;
                        
                        // Process pages up to the configured limit
                        while (currentPage < totalPages && pagesFetched < MAX_PAGES) {
                            try {
                                console.log(`Fetching page ${currentPage + 1} for state ${state}...`);
                                
                                // Build query parameters for pagination
                                const params = {
                                    page: currentPage,
                                    size: pageSize
                                };
                                
                                console.log(`Request URL: ${fullUrl}`);
                                console.log(`Request params: ${JSON.stringify(params)}`);
                                
                                // Make the API request
                                const response = await api.get(fullUrl, {
                                    params,
                                    timeout: 120000  // 2-minute timeout for potentially large responses
                                });
                                
                                console.log(`Response status for page ${currentPage + 1}, state ${state}: ${response.status}`);
                                
                                // Validate response data
                                if (!response.data) {
                                    console.warn(`Warning: No data received for page ${currentPage + 1}, state ${state}.`);
                                    break; // Stop processing pages for this state if no data is returned
                                }
                                
                                if (response.data.error) {
                                    console.error(`API Error for page ${currentPage + 1}, state ${state}:`, response.data.error);
                                    break; // Stop processing pages for this state on API error
                                }
                                
                                // Add detailed logging for data structure
                                console.log(`DETAILED DATA STRUCTURE for ${state}:`, {
                                    responseKeys: Object.keys(response.data),
                                    hasContent: !!response.data.content,
                                    contentLength: response.data.content ? response.data.content.length : 0,
                                    firstItem: response.data.content && response.data.content.length > 0 ? 
                                             JSON.stringify(response.data.content[0]).substring(0, 500) : "No items",
                                    totalPages: response.data.pages || 0,
                                    totalElements: response.data.totalElements || 0
                                });
                                
                                // If we have data but not in expected structure, try to handle it
                                if (!response.data.content || !Array.isArray(response.data.content)) {
                                    // Try to handle alternative response structures
                                    console.log('Attempting to parse alternative response structure');
                                    
                                    // Common alternatives:
                                    // 1. Direct array of subscribers
                                    if (Array.isArray(response.data)) {
                                        console.log('Found array response structure');
                                        response.data = { 
                                            content: response.data,
                                            pages: 1
                                        };
                                    } 
                                    // 2. Data wrapped in 'data' field
                                    else if (response.data.data && (Array.isArray(response.data.data) || response.data.data.content)) {
                                        console.log('Found data field structure');
                                        if (Array.isArray(response.data.data)) {
                                            response.data = {
                                                content: response.data.data,
                                                pages: 1
                                            };
                                        } else {
                                            response.data = response.data.data;
                                        }
                                    }
                                    // 3. Data in 'subscribers' field
                                    else if (response.data.subscribers && Array.isArray(response.data.subscribers)) {
                                        console.log('Found subscribers field structure');
                                        response.data = {
                                            content: response.data.subscribers,
                                            pages: 1
                                        };
                                    }
                                    // If still no valid content structure
                                    if (!response.data.content || !Array.isArray(response.data.content)) {
                                        console.warn(`Warning: Could not parse response structure for state ${state}`);
                                        console.log('Response keys:', Object.keys(response.data));
                                        break;
                                    }
                                }
                                
                                // Process the subscriber data if it has the expected structure
                                if (response.data.content && Array.isArray(response.data.content)) {
                                    // Update total pages on first successful request for this state
                                    if (currentPage === 0) {
                                        totalPages = response.data.pages || 1;
                                        console.log(`Total pages for state ${state}: ${totalPages}`);
                                    }
                                    
                                    const subscribers = response.data.content;
                                    // Log empty content for debugging
                                    if (subscribers.length === 0) {
                                        console.log(`---> Page ${currentPage + 1} for state ${state} returned 0 subscribers (empty content array).`);
                                        console.log('Full response data for empty content:', JSON.stringify(response.data, null, 2));
                                    }
                                    console.log(`Processing ${subscribers.length} subscribers from page ${currentPage + 1}, state ${state}`);
                                    
                                    // Store current result size to track new additions
                                    const previousResultSize = results.size;
                                    
                                    let subscribersWithNumbers = 0;
                                    let totalNumbers = 0;
                                    
                                    // Process each subscriber record
                                    for (const subscriber of subscribers) {
                                        // Each subscriber may have multiple phone numbers
                                        if (subscriber.numbers && Array.isArray(subscriber.numbers) && subscriber.numbers.length > 0) {
                                            subscribersWithNumbers++;
                                            totalNumbers += subscriber.numbers.length;
                                            
                                            subscriber.numbers.forEach(number => {
                                                // Create standardized record format for each number, using Python field names where possible
                                                const record = {
                                                    msisdn: number.msisdn,
                                                    subscriber_id: subscriber.id,
                                                    subscriber_type: 'natural',
                                                    number_state: number.state,
                                                    subscriber_verification_state: subscriber.verificationState,
                                                    subscriber_verification_reason: subscriber.verificationReason,
                                                    first_name: subscriber.firstName,
                                                    last_name: subscriber.lastName,
                                                    date_of_birth: subscriber.dateOfBirth,
                                                    registration_date: '',
                                                    registration_number: number.registrationNumber || '',
                                                    country: subscriber.country,
                                                    sex: subscriber.sex,
                                                    document_type: subscriber.subscriberDocuments?.[0]?.documentType,
                                                    document_number: subscriber.subscriberDocuments?.[0]?.documentNumber,
                                                    document_issue_date: subscriber.subscriberDocuments?.[0]?.dateOfIssue,
                                                    document_expiry_date: subscriber.subscriberDocuments?.[0]?.expiryDate,
                                                    address_plot: subscriber.addresses?.[0]?.plotWardBox,
                                                    address_city: subscriber.addresses?.[0]?.cityTown,
                                                    created_date: subscriber.created,
                                                    updated_date: subscriber.updated
                                                };
                                                results.add(JSON.stringify(record));
                                                
                                                // Also create a Python-compatible format to help debug
                                                const pythonRecord = {
                                                    firstname: subscriber.firstName || '',
                                                    lastname: subscriber.lastName || '',
                                                    dateOfBirth: subscriber.dateOfBirth || '',
                                                    country: subscriber.country || '',
                                                    documentType: subscriber.subscriberDocuments?.[0]?.documentType || '',
                                                    documentNumber: subscriber.subscriberDocuments?.[0]?.documentNumber || '',
                                                    expiryDate: subscriber.subscriberDocuments?.[0]?.expiryDate || '',
                                                    dateOfIssue: subscriber.subscriberDocuments?.[0]?.dateOfIssue || '',
                                                    sex: subscriber.sex || '',
                                                    state: number.state || '',
                                                    updated: subscriber.updated || '',
                                                    msisdn: number.msisdn || '',
                                                    naturalSubscriber: number.naturalSubscriber || '',
                                                    juristicSubscriber: number.juristicSubscriber || '',
                                                    registrationNumber: number.registrationNumber || '',
                                                    verificationReason: subscriber.verificationReason || '',
                                                    verificationState: subscriber.verificationState || ''
                                                };
                                                
                                                // Log a sample record for debugging
                                                if (results.size === 1) {
                                                    console.log('Sample Python-compatible record:', JSON.stringify(pythonRecord, null, 2));
                                                }
                                            });
                                        } else {
                                            console.log(`WARNING: Subscriber without numbers or empty numbers array: ${JSON.stringify(subscriber).substring(0, 300)}...`);
                                        }
                                    }
                                    
                                    console.log(`SUMMARY for page ${currentPage + 1}, state ${state}:`);
                                    console.log(`- Total subscribers: ${subscribers.length}`);
                                    console.log(`- Subscribers with numbers: ${subscribersWithNumbers}`);
                                    console.log(`- Total MSISDN numbers: ${totalNumbers}`);
                                    console.log(`- Records added: ${results.size - previousResultSize}`);
                                    
                                    currentPage++;
                                    pagesFetched++;
                                    console.log(`Processed ${results.size} total records so far (across all states)`);
                                    
                                } else {
                                    // Handle unexpected response structure
                                    console.warn(`Warning: Unexpected response structure for page ${currentPage + 1}, state ${state}. Keys:`, Object.keys(response.data));
                                    console.log('Full response data for unexpected structure:', JSON.stringify(response.data, null, 2));
                                    break; // Stop processing pages for this state
                                }
                                
                            } catch (error) {
                                // Handle critical errors during API request
                                console.error(`!!!!!!!! CRITICAL ERROR fetching page ${currentPage + 1} for state ${state} !!!!!!!!!!`);
                                if (axios.isAxiosError(error)) {
                                    console.error('Axios Error Details:', {
                                        message: error.message,
                                        code: error.code,
                                        status: error.response?.status,
                                        responseData: error.response?.data
                                    });
                                } else {
                                    console.error('Non-Axios Error:', error);
                                }
                                console.error('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
                                break; // Stop processing pages for this state on critical error
                            }
                        }
                        
                        // If this endpoint was successful in retrieving data, mark as successful and stop trying other endpoints
                        if (results.size > 0) {
                            endpointSuccess = true;
                            anyStateSuccessful = true;
                        }
                        
                    } catch (error) {
                        console.error(`Failed to process endpoint ${fullUrl}:`, error.message);
                        // Continue to next endpoint
                    }
                }
            }
            
            // If all endpoints failed, try the MSISDN range method as a last resort
            if (!anyStateSuccessful) {
                console.log("\n========== No endpoints successful. Throwing error ==========");
                throw new Error(`ERROR: Failed to fetch ${type} subscribers with state(s): ${statesToProcess.join(', ')}. All API endpoints failed.`);
            }
            
            console.log(`Completed natural subscribers fetch. Found ${results.size} records.`);
            return results;
        }
        // JURISTIC SUBSCRIBERS HANDLING
        else {
            let url = `/api/v1/juristic_subscribers/mno`;
            console.log(`Using endpoint: ${url}`);
            
            // Initialize pagination variables
            let currentPage = 0;
            const pageSize = PAGE_SIZE;
            let totalPages = 1;
            
            // Process pages up to the configured limit
            while (currentPage < totalPages && currentPage < MAX_PAGES) {
                console.log(`Fetching page ${currentPage + 1} (max: ${MAX_PAGES})`);
                const response = await api.get(url, {
                    params: {
                        page: currentPage,
                        size: pageSize
                    }
                });
                
                // Validate response data
                if (!response.data || response.data.error) {
                    console.error('Error response:', response.data);
                    break;
                }
                
                // Process the juristic subscriber data if it has the expected structure
                if (response.data.content && Array.isArray(response.data.content)) {
                    // Update total pages on first request
                    if (currentPage === 0) {
                        totalPages = response.data.pages || 1;
                        console.log(`Total pages: ${totalPages}`);
                    }
                    
                    const subscribers = response.data.content;
                    console.log(`Processing ${subscribers.length} subscribers from page ${currentPage + 1}`);
                    
                    // Process each juristic subscriber
                    for (const subscriber of subscribers) {
                        if (subscriber.registrationNumber) {
                            try {
                                // Fetch additional details for this juristic subscriber
                                const details = await api.get(`/api/v1/juristic_subscribers/registration_number/${subscriber.registrationNumber}`);
                                console.log(`Juristic details response structure:`, {
                                    hasData: !!details.data,
                                    hasContactPersons: !!details.data?.contactPersons,
                                    contactPersonsIsArray: Array.isArray(details.data?.contactPersons),
                                    contactPersonsLength: Array.isArray(details.data?.contactPersons) ? details.data.contactPersons.length : 'N/A',
                                    keys: details.data ? Object.keys(details.data) : []
                                });
                                
                                // Fetch phone numbers for this juristic subscriber
                                const numbersDetails = await api.get(`/api/v1/juristic_subscribers/numbers/${subscriber.registrationNumber}`);
                                console.log(`Juristic numbers response structure:`, {
                                    hasData: !!numbersDetails.data,
                                    hasNumbers: !!numbersDetails.data?.numbers,
                                    numbersIsArray: Array.isArray(numbersDetails.data?.numbers),
                                    numbersLength: Array.isArray(numbersDetails.data?.numbers) ? numbersDetails.data.numbers.length : 'N/A',
                                    keys: numbersDetails.data ? Object.keys(numbersDetails.data) : []
                                });
                                
                                // Extract contact person data, handling different API response structures
                                const contactPerson = details.data?.contactPersons && details.data.contactPersons.length > 0
                                    ? details.data.contactPersons[0]
                                    : details.data?.contactPerson || {}; // Try alternate property name
                                    
                                console.log(`Contact person data:`, contactPerson);
                                
                                // Extract document data, handling different API response structures
                                const documents = contactPerson.documents && contactPerson.documents.length > 0
                                    ? contactPerson.documents[0]
                                    : contactPerson.document || {}; // Try alternate property name
                                
                                // Extract numbers data, handling different API response structures
                                const numbers = numbersDetails.data?.numbers || 
                                               numbersDetails.data?.msisdns || 
                                               (Array.isArray(numbersDetails.data) ? numbersDetails.data : []);
                                
                                // Process each number for this juristic subscriber
                                if (numbers && Array.isArray(numbers) && numbers.length > 0) {
                                    numbers.forEach(number => {
                                        // Create standardized record format for each number
                                        const record = {
                                            msisdn: number.msisdn,
                                            subscriber_id: subscriber.id,
                                            subscriber_type: 'juristic',
                                            number_state: number.status,
                                            subscriber_verification_state: contactPerson.verificationStatus || '',
                                            subscriber_verification_reason: contactPerson.verificationReason || '',
                                            first_name: contactPerson.firstName || '',
                                            last_name: contactPerson.lastName || '',
                                            date_of_birth: contactPerson.dateOfBirth || '',
                                            registration_date: '',
                                            registration_number: subscriber.registrationNumber,
                                            country: contactPerson.country || '',
                                            sex: contactPerson.sex || '',
                                            document_type: documents.documentType || '',
                                            document_number: documents.documentNumber || '',
                                            document_issue_date: documents.dateOfIssue || '',
                                            document_expiry_date: documents.expiryDate || '',
                                            address_plot: '',
                                            address_city: '',
                                            created_date: subscriber.created || '',
                                            updated_date: subscriber.updated || ''
                                        };
                                        results.add(JSON.stringify(record));
                                    });
                                }
                            } catch (error) {
                                console.error(`Error processing juristic subscriber ${subscriber.registrationNumber}:`, error.message);
                            }
                        }
                    }
                    
                    currentPage++;
                    console.log(`Processed ${results.size} total records so far`);
                    
                } else {
                    // Handle unexpected response structure
                    console.log('No content array in response or unexpected structure');
                    console.log('Response keys:', Object.keys(response.data));
                    break;
                }
            }
            
            // Log if we've reached the page limit
            if (currentPage >= MAX_PAGES && totalPages > MAX_PAGES) {
                console.log(`Reached maximum page limit (${MAX_PAGES}). Skipping remaining ${totalPages - MAX_PAGES} pages.`);
            }
            
            console.log(`Completed juristic subscribers fetch. Found ${results.size} records.`);
            return results;
        }
    } catch (error) {
        // Handle overall errors for the subscriber fetch process
        console.error('Error fetching subscribers:', error.message);
        if (error.response) {
            console.error('Error details:', {
                status: error.response.status,
                data: error.response.data,
                headers: error.response.headers
            });
        }
        throw error;
    }
}

/**
 * Fetches natural subscribers by searching through a range of MSISDNs
 * This is an alternative approach when the verification endpoint doesn't work
 * @param {string|null} verificationState - Verification state to filter by
 * @returns {Set<string>} Set of JSON-stringified subscriber records
 */
async function fetchSubscribersByMsisdnRange(verificationState = null) {
    const results = new Set();
    console.log('Fetching natural subscribers by MSISDN range...');
    
    // We'll try a smaller range first for testing
    const startRange = 70000000; // Start with the general range
    const endRange = 70001000;   // Just try 1,000 numbers for initial test
    const batchSize = 10;        // Smaller batch size to avoid overwhelming the API
    
    let totalProcessed = 0;
    let successCount = 0;
    
    for (let msisdn = startRange; msisdn <= endRange; msisdn += batchSize) {
        const batchEnd = Math.min(msisdn + batchSize - 1, endRange);
        console.log(`Processing MSISDN batch: ${msisdn} - ${batchEnd}`);
        
        const batch = [];
        for (let i = msisdn; i <= batchEnd; i++) {
            batch.push(i);
        }
        
        try {
            // Use Promise.all to process multiple MSISDNs in parallel
            const batchResults = await Promise.all(
                batch.map(async (num) => {
                    try {
                        // Try several different endpoint patterns
                        const possibleEndpoints = [
                            `/api/v1/sim/lookup/${num}`,                         // Try SIM lookup endpoint
                            `/api/v1/subscribers/lookup/${num}`,                  // Generic lookup
                            `/api/v1/customers/msisdn/${num}`,                    // Customer lookup by MSISDN
                            `/api/v1/natural_subscribers/details/msisdn/${num}`,  // Detailed endpoint
                            `/api/v1/subscriber/msisdn/${num}`                    // Singular form
                        ];
                        
                        // Try each endpoint until one works
                        let subscriber = null;
                        for (const endpoint of possibleEndpoints) {
                            try {
                                console.log(`Trying endpoint ${endpoint} for MSISDN ${num}`);
                                const lookupResponse = await api.get(endpoint);
                                
                                if (lookupResponse.data && !lookupResponse.data.error && 
                                    !lookupResponse.data.status && lookupResponse.data.status !== 404) {
                                    console.log(`Found valid response from ${endpoint}`);
                                    subscriber = lookupResponse.data;
                                    break;
                                }
                            } catch (endpointError) {
                                // Continue to next endpoint
                                console.log(`Endpoint ${endpoint} failed for MSISDN ${num}`);
                            }
                        }
                        
                        if (!subscriber) {
                            return null;
                        }
                        
                        // Check verification state if specified
                        if (verificationState && 
                            subscriber.verificationState !== verificationState && 
                            subscriber.verificationStatus !== verificationState) {
                            return null;
                        }
                        
                        // Create record
                        const record = {
                            msisdn: num,
                            subscriber_id: subscriber.id || '',
                            subscriber_type: 'natural',
                            number_state: subscriber.numberState || subscriber.state || '',
                            subscriber_verification_state: subscriber.verificationState || subscriber.verificationStatus || '',
                            subscriber_verification_reason: subscriber.verificationReason || '',
                            first_name: subscriber.firstName || '',
                            last_name: subscriber.lastName || '',
                            date_of_birth: subscriber.dateOfBirth || '',
                            registration_date: subscriber.registrationDate || '',
                            registration_number: '',
                            country: subscriber.country || '',
                            sex: subscriber.sex || '',
                            document_type: subscriber.documentType || (subscriber.subscriberDocuments && subscriber.subscriberDocuments[0] ? subscriber.subscriberDocuments[0].documentType : ''),
                            document_number: subscriber.documentNumber || (subscriber.subscriberDocuments && subscriber.subscriberDocuments[0] ? subscriber.subscriberDocuments[0].documentNumber : ''),
                            document_issue_date: subscriber.dateOfIssue || (subscriber.subscriberDocuments && subscriber.subscriberDocuments[0] ? subscriber.subscriberDocuments[0].dateOfIssue : ''),
                            document_expiry_date: subscriber.expiryDate || (subscriber.subscriberDocuments && subscriber.subscriberDocuments[0] ? subscriber.subscriberDocuments[0].expiryDate : ''),
                            address_plot: (subscriber.addresses && subscriber.addresses[0] ? subscriber.addresses[0].plotWardBox : ''),
                            address_city: (subscriber.addresses && subscriber.addresses[0] ? subscriber.addresses[0].cityTown : ''),
                            created_date: subscriber.created || '',
                            updated_date: subscriber.updated || ''
                        };
                        
                        return JSON.stringify(record);
                    } catch (error) {
                        // Silently fail for individual MSISDNs
                        return null;
                    }
                })
            );
            
            // Filter out null results and add to final results
            batchResults.filter(result => result !== null).forEach(result => {
                results.add(result);
                successCount++;
            });
            
            totalProcessed += batch.length;
            console.log(`Processed ${totalProcessed} MSISDNs, found ${successCount} subscribers`);
            
            // If we've found a good number of subscribers, we can stop
            if (successCount > 1000) {
                console.log(`Found ${successCount} subscribers, stopping early`);
                break;
            }
            
        } catch (error) {
            console.error(`Error processing batch ${msisdn}-${batchEnd}:`, error.message);
            // Continue with next batch
        }
    }
    
    console.log(`Completed MSISDN range scan. Found ${results.size} natural subscribers`);
    return results;
}

/**
 * Main function to download both natural and juristic subscribers
 * Creates CSV files, processes subscribers, and uploads to BDAP server
 * @param {Object} config - Configuration options
 * @returns {boolean} True if download successful
 */
async function downloadSubscribers(config = {}) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const {
        subscriberTypes = ['natural', 'juristic'],
        verificationStates = ['VERIFIED', 'FAILED_VERIFICATION', 'PENDING_VERIFICATION'],
        fromDate = HISTORICAL_START_DATE,
        notificationContact = DEFAULT_CONTACT
    } = config;
    
    console.log('Starting downloadSubscribers function with config:', {
        subscriberTypes,
        verificationStates,
        fromDate,
        timestampUsed: timestamp
    });

    // Create output directories
    const outputPath = await getDatePartitionedPath();
    console.log('Using output path:', outputPath);
    
    // Track generated files
    const generatedFiles = [];
    
    // First, handle natural subscribers using the implementation from server_2.js
    if (subscriberTypes.includes('natural')) {
        for (const status of verificationStates) {
            try {
                console.log(`Processing natural subscribers with status: ${status}`);
                const outputFilename = `${status.toLowerCase()}_natural-subscriber_${timestamp}.csv`;
                const filePath = path.join(outputPath, outputFilename);
                
                // Initialize CSV file
                const initialRow = 'firstname,lastname,dateOfBirth,country,documentType,documentNumber,expiryDate,dateOfIssue,sex,state,updated,msisdn,naturalSubscriber,juristicSubscriber,registrationNumber,verificationReason,verificationState';
                await writeToFile(filePath, initialRow);
                
                // Send notification
                await safeSendNotification(`Starting download for ${status} natural subscribers`, notificationContact);
                
                // Fetch initial data to get pagination details
                const initialResponse = await withRetry(() =>
                    naturalApiClient.get(`/api/v1/natural_subscribers/mno/verification/${status}${DATE_RANGE}?size=2000`)
                );
                
                // Get total pages from response or use default
                const totalPages = initialResponse.data.pages || 3;
                console.log(`Processing ${totalPages} pages of ${status} natural subscribers`);
                let recordCount = 0;
                
                // Process all pages
                for (let currentPage = 0; currentPage < Math.min(totalPages, MAX_PAGES); currentPage++) {
                    try {
                        console.log(`Processing page ${currentPage + 1}/${Math.min(totalPages, MAX_PAGES)}`);
                        const pageData = await withRetry(() =>
                            naturalApiClient.get(`/api/v1/natural_subscribers/mno/verification/${status}${DATE_RANGE}?size=2000&page=${currentPage}`)
                        );
                        
                        if (!pageData.data.content?.length) {
                            console.log(`No content found in page ${currentPage}`);
                            continue;
                        }
                        
                        const records = pageData.data.content.flatMap(subscriber =>
                            subscriber.numbers.map(number => ({
                                firstname: subscriber.firstName || '',
                                lastname: subscriber.lastName || '',
                                dateOfBirth: subscriber.dateOfBirth || '',
                                country: subscriber.country || '',
                                documentType: subscriber.subscriberDocuments?.[0]?.documentType || '',
                                documentNumber: subscriber.subscriberDocuments?.[0]?.documentNumber || '',
                                expiryDate: subscriber.subscriberDocuments?.[0]?.expiryDate || '',
                                dateOfIssue: subscriber.subscriberDocuments?.[0]?.dateOfIssue || '',
                                sex: subscriber.sex || '',
                                state: number.state || '',
                                updated: subscriber.updated || '',
                                msisdn: number.msisdn || '',
                                naturalSubscriber: number.naturalSubscriber || '',
                                juristicSubscriber: number.juristicSubscriber || '',
                                registrationNumber: number.registrationNumber || '',
                                verificationReason: subscriber.verificationReason || '',
                                verificationState: subscriber.verificationState || ''
                            }))
                        );
                        
                        // Format and write records
                        for (const record of records) {
                            // Clean values to prevent CSV corruption
                            const cleanedValues = Object.values(record).map(value => 
                                String(value || '').replace(/,/g, ';').replace(/[\r\n]/g, ' ')
                            );
                            const row = cleanedValues.join(',');
                            await writeToFile(filePath, row);
                            recordCount++;
                        }
                        
                        console.log(`Processed ${records.length} records from page ${currentPage + 1}`);
                        
                    } catch (pageError) {
                        console.error(`Error processing page ${currentPage} for ${status}:`, pageError.message);
                        await safeSendNotification(`Error processing page ${currentPage} for ${status} natural subscribers`, notificationContact);
                    }
                }
                
                console.log(`Completed download for ${status} natural subscribers: ${recordCount} records`);
                generatedFiles.push({
                    type: 'natural',
                    status,
                    path: filePath,
                    recordCount
                });
                
                // Upload the file to BDAP
                try {
                    await uploadToBDAP(filePath);
                    console.log(`Successfully uploaded ${status} natural subscribers file to BDAP`);
                } catch (uploadError) {
                    console.error(`Error uploading ${status} natural subscribers file:`, uploadError);
                }
                
            } catch (error) {
                console.error(`Critical error processing ${status} natural subscribers:`, error.message);
                await safeSendNotification(`Failed to download ${status} natural subscribers - contact support`, notificationContact);
            }
        }
    }
    
    // Now, continue with the original juristic subscribers logic
    if (subscriberTypes.includes('juristic')) {
        try {
            console.log('\nProcessing juristic subscribers...');
            const outputFilename = `juristic_all_${timestamp}.csv`;
            const filePath = path.join(outputPath, outputFilename);
            
            // Initialize CSV file with appropriate header
            const juristicHeader = 'msisdn,subscriber_id,subscriber_type,number_state,subscriber_verification_state,subscriber_verification_reason,first_name,last_name,date_of_birth,registration_date,registration_number,country,sex,document_type,document_number,document_issue_date,document_expiry_date,address_plot,address_city,created_date,updated_date';
            await fs.mkdir(path.dirname(filePath), { recursive: true });
            await fs.writeFile(filePath, juristicHeader + '\n', 'utf-8');
            
            console.log(`Starting download for juristic subscribers to ${DEFAULT_CONTACT}`);
            
            // Fetch juristic subscribers
            console.log('Fetching all juristic subscribers...');
            const juristicResults = await fetchAllSubscribers(null, 'juristic');
            let recordCount = 0;
            
            if (juristicResults.size > 0) {
                console.log(`Found ${juristicResults.size} juristic subscribers, writing to file...`);
                
                // Process and write each juristic record
                for (const resultStr of juristicResults) {
                    try {
                        const record = JSON.parse(resultStr);
                        
                        // Validate required fields
                        if (!record.msisdn || !record.registration_number) {
                            console.warn('Skipping malformed juristic record:', JSON.stringify({
                                msisdn: record.msisdn,
                                registration_number: record.registration_number
                            }));
                            continue;
                        }
                        
                        // Clean values to prevent CSV corruption
                        const cleanedValues = Object.values(record).map(value => 
                            String(value || '').replace(/,/g, ';').replace(/[\r\n]/g, ' ')
                        );
                        const row = cleanedValues.join(',');
                        // Append to file directly
                        await fs.appendFile(filePath, row + '\n', 'utf-8');
                        recordCount++;
                    } catch (recordError) {
                        console.warn('Error processing juristic record:', recordError.message);
                    }
                }
                
                console.log(`Successfully processed ${recordCount} juristic subscriber records`);
                generatedFiles.push({
                    type: 'juristic',
                    status: 'ALL',
                    path: filePath,
                    recordCount
                });
                
                // Upload file to BDAP
                try {
                    await uploadToBDAP(filePath);
                    console.log('Successfully uploaded juristic subscribers file to BDAP');
                } catch (uploadError) {
                    console.error('Error uploading juristic subscribers file:', uploadError);
                }
            } else {
                console.log('No juristic subscribers found');
            }
            
        } catch (error) {
            console.error('Critical error processing juristic subscribers:', error.message);
            await safeSendNotification('Failed to download juristic subscribers - contact support', notificationContact);
        }
    }
    
    // Print summary of all generated files
    console.log('\n====== DOWNLOAD SUMMARY ======');
    let totalRecords = 0;
    
    for (const file of generatedFiles) {
        console.log(`${file.type.toUpperCase()} ${file.status}: ${file.recordCount} records`);
        totalRecords += file.recordCount;
    }
    
    console.log(`Total records processed: ${totalRecords}`);
    console.log('==============================');
    
    return generatedFiles;
}

/**
 * Downloads all juristic subscribers
 * @returns {Promise<string[]>} Paths to the generated files
 */
async function downloadJuristicSubscribers() {
    console.log('Starting juristic subscriber download...');
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    let results = [];
    
    try {
        // Create output path
        const outputPath = await getDatePartitionedPath();
        const outputFilename = `juristic_all_${timestamp}.csv`;
        const filePath = path.join(outputPath, outputFilename);
        
        // Initialize CSV file with appropriate header
        const juristicHeader = 'msisdn,subscriber_id,subscriber_type,number_state,subscriber_verification_state,subscriber_verification_reason,first_name,last_name,date_of_birth,registration_date,registration_number,country,sex,document_type,document_number,document_issue_date,document_expiry_date,address_plot,address_city,created_date,updated_date';
        await fs.mkdir(path.dirname(filePath), { recursive: true });
        await fs.writeFile(filePath, juristicHeader + '\n', 'utf-8');
        
        console.log(`Starting download for juristic subscribers to ${DEFAULT_CONTACT}`);
        
        // Fetch juristic subscribers
        console.log('Fetching all juristic subscribers...');
        const juristicResults = await fetchAllSubscribers(null, 'juristic');
        let recordCount = 0;
        
        if (juristicResults.size > 0) {
            console.log(`Found ${juristicResults.size} juristic subscribers, writing to file...`);
            
            // Process and write each juristic record
            for (const resultStr of juristicResults) {
                try {
                    const record = JSON.parse(resultStr);
                    
                    // Validate required fields
                    if (!record.msisdn || !record.registration_number) {
                        console.warn('Skipping malformed juristic record:', JSON.stringify({
                            msisdn: record.msisdn,
                            registration_number: record.registration_number
                        }));
                        continue;
                    }
                    
                    // Clean values to prevent CSV corruption
                    const cleanedValues = Object.values(record).map(value => 
                        String(value || '').replace(/,/g, ';').replace(/[\r\n]/g, ' ')
                    );
                    const row = cleanedValues.join(',');
                    // Append to file directly
                    await fs.appendFile(filePath, row + '\n', 'utf-8');
                    recordCount++;
                } catch (recordError) {
                    console.warn('Error processing juristic record:', recordError.message);
                }
            }
            
            console.log(`Successfully processed ${recordCount} juristic subscriber records`);
            results.push(filePath);
            
            // Upload file to BDAP
            try {
                await uploadToBDAP(filePath);
                console.log('Successfully uploaded juristic subscribers file to BDAP');
            } catch (uploadError) {
                console.error('Error uploading juristic subscribers file:', uploadError);
            }
        } else {
            console.log('No juristic subscribers found');
        }
        
        console.log(`\nCompleted juristic subscriber download`);
        return results;
        
    } catch (error) {
        console.error('Error in juristic subscribers download:', error);
        console.error(`Failed to download juristic subscribers - contact support: ${error.message}`);
        throw error;
    }
}

/**
 * Production version of natural subscriber download without page limits
 * Used for scheduled weekly runs
 * @returns {Promise<string[]>} Paths to the generated files
 */
async function downloadAllNaturalSubscribersProduction() {
    console.log('Starting FULL PRODUCTION natural subscriber download...');
    
    // Process all verification states
    const statusesToDownload = ['FAILED_VERIFICATION', 'PENDING_VERIFICATION', 'VERIFIED']; 
    let results = [];
    
    try {
        // Process each state with unlimited pages
        for (const status of statusesToDownload) {
            try {
                console.log(`\n---------> Processing ${status} natural subscribers (FULL PRODUCTION RUN) <---------`);
                
                const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
                const fileNamePrefix = `${status.toLowerCase()}_natural-subscriber`;
                const filename = `${fileNamePrefix}_${timestamp}.csv`;
                const filePath = path.join(NATURAL_OUTPUT_DIR, filename);

                await safeSendNotificationNatural(`Starting FULL download for ${status} subscribers`, DEFAULT_CONTACT);

                // Initialize CSV file
                const initialRow = 'firstname,lastname,dateOfBirth,country,documentType,documentNumber,expiryDate,dateOfIssue,sex,state,updated,msisdn,naturalSubscriber,juristicSubscriber,registrationNumber,verificationReason,verificationState';
                await writeToFileNatural(filePath, initialRow);

                // Fetch initial data to get pagination details
                const initialResponse = await withRetryNatural(() =>
                    naturalApiClient.get(`${NATURAL_BASE_URL}/${status}${DATE_RANGE}?size=2000`)
                );

                // Get actual total pages without limit
                const totalPages = initialResponse.data.pages || MAX_PAGES_FULL;
                console.log(`Processing ALL ${totalPages} pages of ${status} subscribers`);

                // Process all pages without artificial limit
                for (let currentPage = 0; currentPage < totalPages; currentPage++) {
                    try {
                        console.log(`Processing page ${currentPage + 1}/${totalPages}`);
                        const pageData = await withRetryNatural(() =>
                            naturalApiClient.get(`${NATURAL_BASE_URL}/${status}${DATE_RANGE}?size=2000&page=${currentPage}`)
                        );

                        if (!pageData.data.content?.length) {
                            console.log(`No content found in page ${currentPage}`);
                            continue;
                        }

                        const records = pageData.data.content.flatMap(subscriber =>
                            subscriber.numbers.map(number => ({
                                firstname: subscriber.firstName,
                                lastname: subscriber.lastName,
                                dateOfBirth: subscriber.dateOfBirth,
                                country: subscriber.country,
                                documentType: subscriber.subscriberDocuments[0]?.documentType || '',
                                documentNumber: subscriber.subscriberDocuments[0]?.documentNumber || '',
                                expiryDate: subscriber.subscriberDocuments[0]?.expiryDate || '',
                                dateOfIssue: subscriber.subscriberDocuments[0]?.dateOfIssue || '',
                                sex: subscriber.sex,
                                state: number.state,
                                updated: subscriber.updated,
                                msisdn: number.msisdn,
                                naturalSubscriber: number.naturalSubscriber,
                                juristicSubscriber: number.juristicSubscriber,
                                registrationNumber: number.registrationNumber,
                                verificationReason: subscriber.verificationReason,
                                verificationState: subscriber.verificationState
                            }))
                        );

                        records.map((record) => {
                            const row = `${record.firstname},${record.lastname}, ${record.dateOfBirth}, ${record.country}, ${record.documentType}, ${record.documentNumber}, ${record.expiryDate},${record.dateOfIssue}, ${record.sex}, ${record.state}, ${record.updated},${record.msisdn},${record.naturalSubscriber},${record.juristicSubscriber},${record.registrationNumber},${record.verificationReason},${record.verificationState}`;
                            writeToFileNatural(filePath, row);
                        });

                        console.log(`Processed ${records.length} records from page ${currentPage + 1}`);

                    } catch (pageError) {
                        console.error(`Error processing page ${currentPage}:`, pageError);
                        await safeSendNotificationNatural(`Error processing page ${currentPage} for ${status} subscribers`, DEFAULT_CONTACT);
                    }
                }

                await safeSendNotificationNatural(`Successfully downloaded ALL ${status} subscribers`, DEFAULT_CONTACT);
                console.log(`Completed FULL download for ${status} subscribers at: ${filePath}`);
                
                results.push(filePath);
                
                // Upload to BDAP
                try {
                    console.log(`Uploading ${status} file to BDAP...`);
                    await uploadToBDAP(filePath);
                    console.log(`Successfully uploaded ${status} file to BDAP`);
                } catch (uploadError) {
                    console.error(`Error uploading ${status} file:`, uploadError);
                }
                
            } catch (error) {
                console.error(`Error processing ${status} natural subscribers:`, error);
            }
        }
        
        console.log(`\nCompleted FULL natural subscriber download: ${results.length} status types processed`);
        return results;
        
    } catch (error) {
        console.error('Error in FULL natural subscribers download:', error);
        throw error;
    }
}

/**
 * Production version of juristic subscriber download without page limits
 * Used for scheduled weekly runs
 * @returns {Promise<string[]>} Paths to the generated files
 */
async function downloadAllJuristicSubscribersProduction() {
    console.log('Starting FULL PRODUCTION juristic subscriber download...');
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    let results = [];
    
    try {
        // Create output path
        const outputPath = await getDatePartitionedPath();
        const outputFilename = `juristic_all_${timestamp}.csv`;
        const filePath = path.join(outputPath, outputFilename);
        
        // Initialize CSV file with appropriate header
        const juristicHeader = 'msisdn,subscriber_id,subscriber_type,number_state,subscriber_verification_state,subscriber_verification_reason,first_name,last_name,date_of_birth,registration_date,registration_number,country,sex,document_type,document_number,document_issue_date,document_expiry_date,address_plot,address_city,created_date,updated_date';
        await fs.mkdir(path.dirname(filePath), { recursive: true });
        await fs.writeFile(filePath, juristicHeader + '\n', 'utf-8');
        
        console.log(`Starting FULL download for juristic subscribers to ${DEFAULT_CONTACT}`);
        
        // Fetch juristic subscribers - override MAX_PAGES with MAX_PAGES_FULL for production
        console.log('Fetching ALL juristic subscribers without page limits...');
        
        // Custom implementation to fetch ALL juristic subscribers
        const url = `/api/v1/juristic_subscribers/mno`;
        console.log(`Using endpoint: ${url}`);
        
        // Initialize pagination variables
        let currentPage = 0;
        const pageSize = PAGE_SIZE;
        let totalPages = 1;
        let recordCount = 0;
        
        // Process pages without the testing limit
        while (currentPage < totalPages) {
            console.log(`Fetching page ${currentPage + 1}`);
            const response = await api.get(url, {
                params: {
                    page: currentPage,
                    size: pageSize
                }
            });
            
            // Validate response data
            if (!response.data || response.data.error) {
                console.error('Error response:', response.data);
                break;
            }
            
            // Process the juristic subscriber data if it has the expected structure
            if (response.data.content && Array.isArray(response.data.content)) {
                // Update total pages on first request
                if (currentPage === 0) {
                    totalPages = response.data.pages || 1;
                    console.log(`Total pages for juristic subscribers: ${totalPages}`);
                }
                
                const subscribers = response.data.content;
                console.log(`Processing ${subscribers.length} subscribers from page ${currentPage + 1}`);
                
                // Process each juristic subscriber
                for (const subscriber of subscribers) {
                    if (subscriber.registrationNumber) {
                        try {
                            // Fetch additional details for this juristic subscriber
                            const details = await api.get(`/api/v1/juristic_subscribers/registration_number/${subscriber.registrationNumber}`);
                            
                            // Fetch phone numbers for this juristic subscriber
                            const numbersDetails = await api.get(`/api/v1/juristic_subscribers/numbers/${subscriber.registrationNumber}`);
                            
                            // Extract contact person data, handling different API response structures
                            const contactPerson = details.data?.contactPersons && details.data.contactPersons.length > 0
                                ? details.data.contactPersons[0]
                                : details.data?.contactPerson || {}; // Try alternate property name
                                
                            // Extract document data, handling different API response structures
                            const documents = contactPerson.documents && contactPerson.documents.length > 0
                                ? contactPerson.documents[0]
                                : contactPerson.document || {}; // Try alternate property name
                            
                            // Extract numbers data, handling different API response structures
                            const numbers = numbersDetails.data?.numbers || 
                                           numbersDetails.data?.msisdns || 
                                           (Array.isArray(numbersDetails.data) ? numbersDetails.data : []);
                            
                            // Process each number for this juristic subscriber
                            if (numbers && Array.isArray(numbers) && numbers.length > 0) {
                                numbers.forEach(number => {
                                    // Create standardized record format for each number
                                    const record = {
                                        msisdn: number.msisdn,
                                        subscriber_id: subscriber.id,
                                        subscriber_type: 'juristic',
                                        number_state: number.status,
                                        subscriber_verification_state: contactPerson.verificationStatus || '',
                                        subscriber_verification_reason: contactPerson.verificationReason || '',
                                        first_name: contactPerson.firstName || '',
                                        last_name: contactPerson.lastName || '',
                                        date_of_birth: contactPerson.dateOfBirth || '',
                                        registration_date: '',
                                        registration_number: subscriber.registrationNumber,
                                        country: contactPerson.country || '',
                                        sex: contactPerson.sex || '',
                                        document_type: documents.documentType || '',
                                        document_number: documents.documentNumber || '',
                                        document_issue_date: documents.dateOfIssue || '',
                                        document_expiry_date: documents.expiryDate || '',
                                        address_plot: '',
                                        address_city: '',
                                        created_date: subscriber.created || '',
                                        updated_date: subscriber.updated || ''
                                    };
                                    
                                    // Clean values to prevent CSV corruption
                                    const cleanedValues = Object.values(record).map(value => 
                                        String(value || '').replace(/,/g, ';').replace(/[\r\n]/g, ' ')
                                    );
                                    const row = cleanedValues.join(',');
                                    
                                    // Append to file directly
                                    fs.appendFile(filePath, row + '\n', 'utf-8');
                                    recordCount++;
                                });
                            }
                        } catch (subscriberError) {
                            console.error(`Error processing juristic subscriber ${subscriber.registrationNumber}:`, subscriberError.message);
                        }
                    }
                }
                
                currentPage++;
                console.log(`Processed ${recordCount} total juristic records so far`);
                
            } else {
                // Handle unexpected response structure
                console.log('No content array in response or unexpected structure');
                console.log('Response keys:', Object.keys(response.data));
                break;
            }
        }
        
        console.log(`Successfully processed ${recordCount} juristic subscriber records`);
        results.push(filePath);
        
        // Upload file to BDAP
        try {
            await uploadToBDAP(filePath);
            console.log('Successfully uploaded FULL juristic subscribers file to BDAP');
        } catch (uploadError) {
            console.error('Error uploading juristic subscribers file:', uploadError);
        }
        
        console.log(`\nCompleted FULL juristic subscriber download`);
        return results;
        
    } catch (error) {
        console.error('Error in FULL juristic subscribers download:', error);
        console.error(`Failed to download juristic subscribers - contact support: ${error.message}`);
        throw error;
    }
}

/**
 * Schedules weekly download of both natural and juristic subscriber data
 * Runs every Monday at 1 AM with full data download (no page limits)
 */
function scheduleWeeklyDownload() {
    console.log('Scheduling FULL DATA weekly download for Mondays at 1 AM...');
    cron.schedule('0 1 * * 1', async () => {
        console.log('Starting scheduled FULL DATA download at:', new Date().toISOString());
        try {
            // Process all verification states for natural subscribers
            const statusesToDownload = ['FAILED_VERIFICATION', 'PENDING_VERIFICATION', 'VERIFIED'];
            let naturalResults = [];
            
            console.log('***** DOWNLOADING NATURAL SUBSCRIBERS (ALL VERIFICATION STATES) *****');
            
            for (const status of statusesToDownload) {
                try {
                    console.log(`\n---------> Processing ${status} natural subscribers <---------`);
                    const filePath = await downloadNaturalSubscribers({
                        status,
                        fileNamePrefix: `${status.toLowerCase()}_natural-subscriber`,
                        notificationContact: DEFAULT_CONTACT
                    });
                    
                    if (filePath) {
                        naturalResults.push(filePath);
                        
                        // Upload to BDAP
                        try {
                            console.log(`Uploading ${status} natural subscribers file to BDAP...`);
                            await uploadToBDAP(filePath);
                            console.log(`Successfully uploaded ${status} natural subscribers file to BDAP`);
                        } catch (uploadError) {
                            console.error(`Error uploading ${status} natural subscribers file:`, uploadError);
                        }
                    }
                } catch (error) {
                    console.error(`Error processing ${status} natural subscribers:`, error);
                }
            }
            
            console.log(`\nCompleted natural subscriber download: ${naturalResults.length} status types processed`);
            
            // Download juristic subscribers
            console.log('\n***** DOWNLOADING JURISTIC SUBSCRIBERS *****');
            try {
                const juristicFilePath = await downloadJuristicSubscribers();
                
                if (juristicFilePath) {
                    // Upload to BDAP
                    try {
                        console.log('Uploading juristic subscribers file to BDAP...');
                        await uploadToBDAP(juristicFilePath);
                        console.log('Successfully uploaded juristic subscribers file to BDAP');
                    } catch (uploadError) {
                        console.error('Error uploading juristic subscribers file:', uploadError);
                    }
                }
                
                console.log('Juristic subscriber download completed successfully');
            } catch (error) {
                console.error('Error processing juristic subscribers:', error);
            }
            
            // Send notification that weekly download is complete
            await safeSendNotificationNatural(`Weekly BOCRA data sync completed. Processed ${naturalResults.length} natural verification states and juristic subscribers.`, DEFAULT_CONTACT);
            
            console.log('Weekly FULL DATA scheduled download completed successfully');
        } catch (error) {
            console.error('Scheduled FULL DATA download failed:', error);
            await safeSendNotificationNatural(`Weekly BOCRA data sync failed: ${error.message}`, DEFAULT_CONTACT);
        }
    }, {
        scheduled: true,
        timezone: "Africa/Gaborone" // Use Botswana timezone
    });
    
    console.log('Weekly data sync scheduled for Mondays at 1 AM Botswana time');
}

/**
 * Test function focused on natural subscriber downloads
 * @returns {Promise<boolean>} True if test successful
 */
async function runTest() {
    try {
        console.log('STARTING TEST RUN');
        console.log('=================');
        
        // Step 1: Test API connectivity for natural subscribers
        console.log('1. Testing API connectivity for natural subscribers...');
        const apiConnected = await testAPIConnectivity();
        if (!apiConnected) {
            console.warn('⚠️ API connectivity test warning - continuing anyway');
        } else {
            console.log('✓ API connectivity confirmed');
        }
        
        // Step 2: Run natural subscribers download
        console.log('\n2. Running natural subscribers download...');
        await downloadNaturalSubscribersDirectly();
        console.log('✓ Natural subscribers download completed');
        
        // Step 3: Run juristic subscribers download
        console.log('\n3. Running juristic subscribers download...');
        await downloadJuristicSubscribers();
        console.log('✓ Juristic subscribers download completed');
        
        console.log('\nTEST RUN COMPLETED SUCCESSFULLY');
        console.log('==============================');
        return true;
    } catch (error) {
        console.error('\n❌ TEST FAILED:', error.message);
        console.error('Please check the logs for more details');
        return false;
    }
}

/**
 * Tests API connectivity against the BOCRA SIMS API
 * Modified to test only natural subscriber verification endpoint
 * @returns {Promise<boolean>} True if connectivity test is successful
 */
async function testAPIConnectivity() {
    console.log('Testing API connectivity for natural subscribers verification...');
    try {
        // Test only the /natural_subscribers/mno/verification endpoint
        // This is the only endpoint we need for natural subscriber downloads
        const testUrl = `${NATURAL_BASE_URL}/VERIFIED${DATE_RANGE}?size=1`;
        
        console.log(`Testing endpoint: ${testUrl}`);
        const response = await naturalApiClient.get(testUrl);
        
        if (response.status === 200) {
            console.log('✓ Natural subscribers verification endpoint test successful');
            return true;
        } else {
            console.error(`❌ Endpoint test failed with status: ${response.status}`);
            return false;
        }
    } catch (error) {
        console.error('❌ API connectivity test failed:', error.message);
        return false;
    }
}

/**
 * Downloads all natural subscribers directly
 * This is the main function for natural subscriber downloads
 * @returns {Promise<string[]>} Paths to the generated files
 */
async function downloadNaturalSubscribersDirectly() {
    console.log('Starting natural subscriber download...');
    
    // Process all verification states
    const statusesToDownload = ['FAILED_VERIFICATION', 'PENDING_VERIFICATION', 'VERIFIED']; 
    let results = [];
    
    try {
        // Process each state using the exact approach from server_2.js
        for (const status of statusesToDownload) {
            try {
                console.log(`\n---------> Processing ${status} natural subscribers <---------`);
                const filePath = await downloadNaturalSubscribers({
                    status,
                    fileNamePrefix: `${status.toLowerCase()}_natural-subscriber`,
                    notificationContact: DEFAULT_CONTACT
                });
                
                if (filePath) {
                    results.push(filePath);
                    
                    // Enable BDAP upload
                    try {
                        console.log(`Uploading ${status} file to BDAP...`);
                        await uploadToBDAP(filePath);
                        console.log(`Successfully uploaded ${status} file to BDAP`);
                    } catch (uploadError) {
                        console.error(`Error uploading ${status} file:`, uploadError);
                    }
                }
            } catch (error) {
                console.error(`Error processing ${status} natural subscribers:`, error);
            }
        }
        
        console.log(`\nCompleted natural subscriber download: ${results.length} status types processed`);
        return results;
        
    } catch (error) {
        console.error('Error in natural subscribers download:', error);
        throw error;
    }
}

// Run natural subscriber download for all statuses immediately
console.log('Starting immediate natural subscriber download...');
const statusesToDownload = ['FAILED_VERIFICATION', 'PENDING_VERIFICATION', 'VERIFIED'];
statusesToDownload.forEach(async (status) => {
    try {
        await downloadNaturalSubscribers({
            status: status,
            fileNamePrefix: status.toLowerCase() + '_natural-subscriber',
            notificationContact: "+26773001762"
        });
    } catch (error) {
        console.error(`Error downloading ${status} natural subscribers:`, error);
    }
});

/**
 * Test function for production-level full data download
 * @returns {Promise<boolean>} True if test successful
 */
async function runProductionTest() {
    try {
        console.log('STARTING PRODUCTION TEST RUN');
        console.log('============================');
        
        // Step 1: Test API connectivity for natural subscribers
        console.log('1. Testing API connectivity for natural subscribers...');
        const apiConnected = await testAPIConnectivity();
        if (!apiConnected) {
            console.warn('⚠️ API connectivity test warning - continuing anyway');
        } else {
            console.log('✓ API connectivity confirmed');
        }
        
        // Step 2: Run FULL natural subscribers download without page limits
        console.log('\n2. Running FULL natural subscribers download...');
        await downloadAllNaturalSubscribersProduction();
        console.log('✓ FULL natural subscribers download completed');
        
        // Step 3: Run FULL juristic subscribers download without page limits
        console.log('\n3. Running FULL juristic subscribers download...');
        await downloadAllJuristicSubscribersProduction();
        console.log('✓ FULL juristic subscribers download completed');
        
        console.log('\nPRODUCTION TEST RUN COMPLETED SUCCESSFULLY');
        console.log('==========================================');
        return true;
    } catch (error) {
        console.error('\n❌ PRODUCTION TEST FAILED:', error.message);
        console.error('Please check the logs for more details');
        return false;
    }
}

// Add commandline argument support to run production test
if (process.argv.includes('--production')) {
    console.log('Running in production mode with FULL data download');
    runProductionTest().catch(console.error);
    scheduleWeeklyDownload();
} else if (import.meta.url === `file://${process.argv[1]}`) {
    console.log('Running in standard test mode (limited pages)');
    runTest().catch(console.error);
    scheduleWeeklyDownload();
} else {
    console.log('Running as a module');
}

/**
 * Executes a one-time immediate push of all BOCRA subscriber data (all 3 states) to BDAP server
 */
function scheduleOnePushToBDAP() {
    console.log(`Starting immediate push of all BOCRA subscriber data...`);
    
    // Process all verification states immediately instead of scheduling
    (async () => {
        try {
            console.log('Starting immediate push of all BOCRA subscriber data at:', new Date().toISOString());
            
            // Process all verification states
            const statusesToDownload = ['FAILED_VERIFICATION', 'PENDING_VERIFICATION', 'VERIFIED'];
            let results = [];
            
            for (const status of statusesToDownload) {
                try {
                    console.log(`\n---------> Processing ${status} natural subscribers <---------`);
                    const filePath = await downloadNaturalSubscribers({
                        status,
                        fileNamePrefix: `${status.toLowerCase()}_natural-subscriber`,
                        notificationContact: DEFAULT_CONTACT
                    });
                    
                    if (filePath) {
                        results.push(filePath);
                        
                        // Upload to BDAP
                        try {
                            console.log(`Uploading ${status} file to BDAP...`);
                            await uploadToBDAP(filePath);
                            console.log(`Successfully uploaded ${status} file to BDAP`);
                        } catch (uploadError) {
                            console.error(`Error uploading ${status} file:`, uploadError);
                        }
                    }
                } catch (error) {
                    console.error(`Error processing ${status} natural subscribers:`, error);
                }
            }
            
            // Send notification that all files have been processed
            await safeSendNotificationNatural(`BOCRA data push completed. Processed ${results.length} status types with FULL data.`, DEFAULT_CONTACT);
            console.log(`\nCompleted immediate push of BOCRA subscriber data: ${results.length} status types processed`);
            
        } catch (error) {
            console.error('Immediate push failed:', error);
            await safeSendNotificationNatural(`BOCRA data push failed: ${error.message}`, DEFAULT_CONTACT);
        }
    })();
    
    console.log(`Immediate push initiated`);
}

// Schedule the one-time push
scheduleOnePushToBDAP();

// Export functions for external use
export { 
    downloadNaturalSubscribers,
    downloadNaturalSubscribersDirectly,
    downloadJuristicSubscribers,
    downloadAllNaturalSubscribersProduction,
    downloadAllJuristicSubscribersProduction,
    scheduleWeeklyDownload, 
    runTest,
    runProductionTest
};