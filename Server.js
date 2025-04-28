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
const PAGE_SIZE = 2000;  // Number of records to fetch per page
const MAX_PAGES = 3;     // Maximum number of pages to fetch per subscriber type/state

// MSISDN range and retry configuration
const MSISDN_START = 70000000; // Starting MSISDN range for Botswana
const MSISDN_END = 79999999;   // Ending MSISDN range for Botswana
const BATCH_SIZE = 100;        // Number of MSISDNs to process in parallel
const MAX_RETRIES = 3;         // Maximum number of retry attempts
const RETRY_DELAY = 5000;      // Delay between retries (5 seconds)
const CONNECTION_TIMEOUT = 30000; // Connection timeout (30 seconds)

/**
 * Configure Axios instance for API requests
 * Sets up base URL, timeout, and authentication headers
 */
const api = axios.create({
    baseURL: BASE_URL,
    timeout: 30000,
    headers: {
        'x-api-key': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
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
            const dateRange = `/start/${startDate}/end/${endDate}`;
            
            console.log(`Using date range: ${dateRange}`);
            
            // Process each verification state
            for (const state of statesToProcess) {
                console.log("\n========== Processing " + state + " natural subscribers ==========");
                
                // Build URL exactly like the TypeScript code
                const fullUrl = `${NATURAL_API_URL}/${state}${dateRange}`;
                console.log("Using TypeScript-matched URL: " + fullUrl);
                
                // Initialize pagination variables
                let currentPage = 0;
                const pageSize = 2000;
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
                            timeout: 60000  // 60-second timeout for potentially large responses
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
                            
                            // Process each subscriber record
                            for (const subscriber of subscribers) {
                                // Each subscriber may have multiple phone numbers
                                if (subscriber.numbers && Array.isArray(subscriber.numbers)) {
                                    subscriber.numbers.forEach(number => {
                                        // Create standardized record format for each number
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
                                            registration_number: '',
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
                                    });
                                }
                            }
                            
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
                
                // Log if we've reached the page limit
                if (pagesFetched >= MAX_PAGES && totalPages > MAX_PAGES) {
                    console.log(`Reached maximum page limit (${MAX_PAGES}) for state ${state}. Skipping remaining ${totalPages - pagesFetched} pages.`);
                }
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
    const partitionPath = await getDatePartitionedPath(config.date || new Date());
    
    // CSV header
    const header = [
        'msisdn',
        'subscriber_id',
        'subscriber_type',
        'number_state',
        'subscriber_verification_state',
        'subscriber_verification_reason',
        'first_name',
        'last_name',
        'date_of_birth',
        'registration_date',
        'registration_number',
        'country',
        'sex',
        'document_type',
        'document_number',
        'document_issue_date',
        'document_expiry_date',
        'address_plot',
        'address_city',
        'created_date',
        'updated_date'
    ].join(',') + '\n';

    const verificationStates = ['ACTIVE', 'FAILED', 'PENDING'];
    const subscriberTypes = ['natural', 'juristic'];
    let totalProcessed = 0;
    const startTime = Date.now();
    const outputFiles = {};
    const results = {};

    console.log("\n=============== Creating output files ===============");
    // Create files for each type
    for (const state of verificationStates) {
        const fileName = path.join(partitionPath, `natural_${state.toLowerCase()}_${timestamp}.csv`);
        outputFiles[`natural_${state}`] = fileName;
        results[`natural_${state}`] = new Set();
        await fs.writeFile(fileName, header);
        console.log(`Created file for ${state} natural subscribers:`, fileName);
    }

    // Create juristic file
    const juristicFileName = path.join(partitionPath, `juristic_all_${timestamp}.csv`);
    outputFiles['juristic'] = juristicFileName;
    results['juristic'] = new Set();
    await fs.writeFile(juristicFileName, header);
    console.log('Created file for juristic subscribers:', juristicFileName);

    // Process each subscriber type
    for (const type of subscriberTypes) {
        console.log(`\nProcessing ${type} subscribers...`);
        
        if (type === 'natural') {
            // Process natural subscribers by verification state
            for (const state of verificationStates) {
                try {
                    console.log(`\n---------> Attempting to fetch state: ${state} <---------`);
                    const stateResults = await fetchAllSubscribers(state, type);
                    
                    // Add to appropriate result set
                    stateResults.forEach(result => results[`natural_${state}`].add(result));
                    totalProcessed += stateResults.size;
                    
                    // Log progress
                    const elapsed = (Date.now() - startTime) / 1000;
                    console.log(`Progress: ${totalProcessed} total records processed in ${elapsed.toFixed(1)}s`);
                    
                    // Write batch to appropriate file
                    console.log(`Writing ${stateResults.size} ${state} natural subscribers to file...`);
                    for (const resultStr of stateResults) {
                        const record = JSON.parse(resultStr);
                        const line = Object.values(record)
                            .map(field => `"${String(field || '').replace(/"/g, '""')}"`)
                            .join(',') + '\n';
                        await fs.appendFile(outputFiles[`natural_${state}`], line);
                    }
                } catch (error) {
                    console.error(`Error processing ${type} subscribers with ${state} verification state:`, error.message);
                }
            }
        } else {
            // Process juristic subscribers (leave this as is)
            try {
                console.log(`\nFetching all juristic subscribers...`);
                const stateResults = await fetchAllSubscribers(null, type);
                
                // Process and validate each juristic record
                for (const resultStr of stateResults) {
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
                        
                        results['juristic'].add(resultStr);
                        totalProcessed++;
                        
                        // Write valid record to file
                        const line = Object.values(record)
                            .map(field => `"${String(field || '').replace(/"/g, '""')}"`)
                            .join(',') + '\n';
                        await fs.appendFile(outputFiles['juristic'], line);
                    } catch (error) {
                        console.warn('Error processing juristic record:', error.message);
                        continue;
                    }
                }
                
                // Log progress
                const elapsed = (Date.now() - startTime) / 1000;
                console.log(`Progress: ${totalProcessed} total records processed in ${elapsed.toFixed(1)}s`);
                
            } catch (error) {
                console.error(`Error processing ${type} subscribers:`, error.message);
            }
        }
    }

    console.log('\nProcessing completed:');
    console.log('- Natural Subscribers:');
    for (const state of verificationStates) {
        console.log(`  - ${state}: ${results[`natural_${state}`].size} records`);
    }
    console.log(`- Juristic Subscribers: ${results['juristic'].size} records`);
    console.log(`- Total records: ${totalProcessed}`);
    
    // Upload all files to BDAP
    console.log('\nUploading files to BDAP server...');
    for (const [type, file] of Object.entries(outputFiles)) {
        try {
            console.log(`Uploading ${type} file...`);
            await uploadToBDAP(file);
            console.log(`Successfully uploaded ${type} file`);
        } catch (error) {
            console.error(`Error uploading ${type} file:`, error.message);
        }
    }
    
    return true;
}

/**
 * Schedules the weekly download job using node-cron
 * Runs every Monday at 2 AM
 */
function scheduleWeeklyDownload() {
    console.log('Scheduling weekly download for Mondays at 2 AM...');
    cron.schedule('0 2 * * 1', async () => {
        console.log('Starting scheduled download at:', new Date().toISOString());
        try {
            await downloadSubscribers();
            console.log('Scheduled download completed successfully');
        } catch (error) {
            console.error('Scheduled download failed:', error);
        }
    });
}

/**
 * Test function to verify the entire pipeline
 * Validates configuration, tests connectivity, and performs a download
 */
async function runTest() {
    console.log('\n=== Starting Test Run ===');
    try {
        console.log('\nBDAP Configuration:');
        console.log('- Host:', BDAP_CONFIG.host);
        console.log('- Port:', BDAP_CONFIG.port);
        console.log('- Username:', BDAP_CONFIG.username);
        console.log('- Remote Directory:', BDAP_CONFIG.remoteDir);

        // Validate BDAP configuration
        console.log('\nValidating BDAP configuration...');
        validateBDAPConfig();
        console.log('BDAP configuration is valid');
        
        // First test API connectivity with specific test MSISDNs
        console.log('\nTesting API connectivity with specific test MSISDNs...');
        try {
            await testAPIConnectivity();
        } catch (error) {
            console.warn('⚠️ API connectivity test failed, but continuing with main process:', error.message);
        }

        console.log('\nStarting historical data download...');
        await downloadSubscribers();
        
        // Add direct natural subscriber download using TypeScript approach
        console.log('\nAttempting direct download of natural subscribers using TypeScript approach...');
        try {
            await downloadNaturalSubscribersDirectly();
            console.log('✓ Direct natural subscribers download completed');
        } catch (error) {
            console.error('❌ Direct natural subscribers download failed:', error.message);
        }
        
        console.log('\n✅ Historical data download completed');
    } catch (error) {
        console.error('\n❌ Test failed:', error);
        if (error.stack) {
            console.error('\nError stack:', error.stack);
        }
    }
    console.log('\n=== Test Run Complete ===\n');
}

/**
 * Tests API connectivity using predefined test MSISDNs
 * Tries different endpoint patterns to find working ones
 * @returns {string|null} Working endpoint pattern, or null if none found
 */
async function testAPIConnectivity() {
    console.log('Testing API connectivity with predefined test MSISDNs...');
    
    // Test with specific MSISDNs we know exist
    const testMSISDNs = [TEST_MSISDN]; // Using the predefined TEST_MSISDN
    
    // Also try a few endpoints to see which one works
    const testEndpoints = [
        `/api/v1/subscribers/msisdn/${TEST_MSISDN}`,
        `/api/v1/natural_subscribers/msisdn/${TEST_MSISDN}`,
        `/api/v1/sim/lookup/${TEST_MSISDN}`,
        `/api/v1/subscribers/lookup/${TEST_MSISDN}`,
        `/api/v1/subscriber/msisdn/${TEST_MSISDN}`,
        `/api/v1/natural_subscribers/details/msisdn/${TEST_MSISDN}`
    ];
    
    console.log(`Testing endpoints with MSISDN ${TEST_MSISDN}...`);
    
    // Try each endpoint
    for (const endpoint of testEndpoints) {
        try {
            console.log(`\nTrying endpoint: ${endpoint}`);
            const response = await api.get(endpoint);
            
            console.log('Response status:', response.status);
            if (response.data) {
                console.log('Response has data:', {
                    dataKeys: Object.keys(response.data),
                    isArray: Array.isArray(response.data),
                    hasError: !!response.data.error,
                    status: response.data.status
                });
                
                // If this seems like a valid subscriber response, log more details
                if (!response.data.error && response.status === 200) {
                    console.log('✅ Found working endpoint:', endpoint);
                    console.log('Subscriber data sample:', JSON.stringify(response.data, null, 2).substring(0, 500) + '...');
                    
                    // Save this endpoint for future use
                    console.log('RECOMMENDED: Use this endpoint pattern for natural subscribers');
                    return endpoint;
                }
            } else {
                console.log('❌ No data in response');
            }
        } catch (error) {
            console.log(`❌ Endpoint ${endpoint} failed:`, {
                status: error.response?.status,
                message: error.message,
                data: error.response?.data
            });
        }
    }
    
    console.log('\n⚠️ Warning: No working endpoint found for natural subscribers with MSISDN lookup');
    console.log('Continuing with direct verification endpoint methods...');
    return null;
}

/**
 * Direct implementation of natural subscriber download
 * Uses the same approach as the TypeScript implementation
 */
async function downloadNaturalSubscribersDirectly() {
    console.log('Starting direct natural subscriber download...');
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const partitionPath = await getDatePartitionedPath();
    
    // Convert date format for API (YYYY-MM-DD to MM-DD-YYYY)
    const formatDateForApi = (dateStr) => {
        const [year, month, day] = dateStr.split('-');
        return `${month}-${day}-${year}`;
    };
    
    const startDate = formatDateForApi(HISTORICAL_START_DATE);
    const endDate = formatDateForApi(HISTORICAL_END_DATE);
    const dateRange = `/start/${startDate}/end/${endDate}`;
    
    // CSV header for natural subscribers
    const header = [
        'firstname',
        'lastname',
        'dateOfBirth',
        'country',
        'documentType',
        'documentNumber',
        'expiryDate',
        'dateOfIssue',
        'sex',
        'state',
        'updated',
        'msisdn',
        'naturalSubscriber',
        'juristicSubscriber',
        'registrationNumber',
        'verificationReason',
        'verificationState'
    ].join(',') + '\n';
    
    // Subscriber states to process
    const states = {
        'VERIFIED': 'verified_natural-subscriber',
        'FAILED_VERIFICATION': 'failed_natural-subscriber',
        'PENDING_VERIFICATION': 'pending_natural-subscriber'
    };
    
    const outputFiles = {};
    let totalProcessed = 0;
    
    // Create output files for each state
    for (const [state, filePrefix] of Object.entries(states)) {
        const filePath = path.join(partitionPath, `${filePrefix}_${timestamp}.csv`);
        outputFiles[state] = filePath;
        await fs.writeFile(filePath, header);
        console.log(`Created file for ${state} natural subscribers:`, filePath);
    }
    
    // Process each state
    for (const [state, filePrefix] of Object.entries(states)) {
        console.log(`\n========== Processing ${state} natural subscribers using direct approach ==========`);
        
        try {
            // Build URL exactly like TypeScript code
            const url = `${NATURAL_API_URL}/${state}${dateRange}`;
            console.log(`Using URL: ${url}`);
            
            // Get initial response to determine pagination
            const initialResponse = await api.get(url, {
                params: {
                    size: 2000
                },
                timeout: 60000 // 60s timeout
            });
            
            if (!initialResponse.data) {
                console.warn(`No data returned for ${state}`);
                continue;
            }
            
            // Log first response structure
            console.log(`Response structure for ${state}:`, {
                hasContent: !!initialResponse.data.content,
                contentIsArray: Array.isArray(initialResponse.data.content),
                contentLength: Array.isArray(initialResponse.data.content) ? initialResponse.data.content.length : 'N/A',
                totalPages: initialResponse.data.pages || 0,
                keys: Object.keys(initialResponse.data)
            });
            
            const totalPages = initialResponse.data.pages || 1;
            console.log(`Total pages for ${state}: ${totalPages}`);
            
            // Process up to MAX_PAGES pages
            const pagesToProcess = Math.min(totalPages, MAX_PAGES);
            console.log(`Will process ${pagesToProcess} pages for ${state}`);
            
            let stateRecords = 0;
            
            // Process each page
            for (let currentPage = 0; currentPage < pagesToProcess; currentPage++) {
                console.log(`Fetching page ${currentPage + 1}/${pagesToProcess} for ${state}...`);
                
                const pageResponse = currentPage === 0 ? initialResponse : 
                    await api.get(url, {
                        params: {
                            size: 2000,
                            page: currentPage
                        },
                        timeout: 60000
                    });
                
                if (!pageResponse.data.content || !Array.isArray(pageResponse.data.content)) {
                    console.warn(`No content array in page ${currentPage + 1}`);
                    continue;
                }
                
                const subscribers = pageResponse.data.content;
                console.log(`Processing ${subscribers.length} subscribers from page ${currentPage + 1}`);
                
                // Using exact logic from TypeScript code
                const records = subscribers.flatMap(subscriber =>
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
                
                // Write records to file
                for (const record of records) {
                    const row = Object.values(record)
                        .map(field => `"${String(field || '').replace(/"/g, '""')}"`)
                        .join(',') + '\n';
                    await fs.appendFile(outputFiles[state], row);
                    stateRecords++;
                    totalProcessed++;
                }
                
                console.log(`Processed ${stateRecords} records for ${state} so far`);
            }
            
            console.log(`Completed ${state}: ${stateRecords} records`);
            
        } catch (error) {
            console.error(`Error processing ${state}:`, error.message);
            if (error.response) {
                console.error('Error details:', {
                    status: error.response.status,
                    data: error.response.data,
                    headers: error.response.headers
                });
            }
        }
    }
    
    console.log(`\nCompleted direct natural subscriber download: ${totalProcessed} total records`);
    
    // Upload the files to BDAP
    console.log('\nUploading natural subscriber files to BDAP server...');
    for (const [state, filePath] of Object.entries(outputFiles)) {
        try {
            console.log(`Uploading ${state} file...`);
            await uploadToBDAP(filePath);
            console.log(`Successfully uploaded ${state} file`);
        } catch (error) {
            console.error(`Error uploading ${state} file:`, error.message);
        }
    }
}

// Entry point - runs when the script is executed directly
const isMainModule = process.argv[1] && process.argv[1].endsWith('Server.js');
if (isMainModule) {
    console.log('Running in standalone mode');
    runTest().catch(console.error);
    // Also start the scheduled task
    scheduleWeeklyDownload();
} else {
    console.log('Running as a module');
}

// Export functions for external use
export { downloadSubscribers, scheduleWeeklyDownload, runTest };