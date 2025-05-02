import axios from 'axios';
import path from 'path';
import fs from "fs";

const API_KEY = 'BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA='
const DEFAULT_CONTACT = "+26773001762";
const BASE_URL = 'https://simsapi.bocra.org.bw/api/v1/natural_subscribers/mno/verification';
// const OUTPUT_DIR = path.resolve(__dirname, "resources/");
const OUTPUT_DIR = "resources/";
const DATE_RANGE = '/start/01-01-2022/end/01-01-2026';


const apiClient = axios.create({
    headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
    },
    timeout: 30000,
});


async function writeToFile(filePath, content) {
    // Ensure output directory exists
    if (!fs.existsSync(OUTPUT_DIR)) {
        fs.mkdirSync(OUTPUT_DIR);
    }
    await fs.promises.appendFile(filePath, content + "\n", "utf-8");
}

async function withRetry(fn, retries = 3) {
    try {
        return await fn();
    } catch (error) {
        if (retries > 0) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            return withRetry(fn, retries - 1);
        }
        throw error;
    }
}

async function safeSendNotification(message, contact) {
    try {
        // await sendNotification(message, contact);
        console.log(`Sending ${message} to ${contact}`);
    } catch (error) {
        console.error('Failed to send notification:', error);
    }
}

async function downloadSubscribers(config) {
    const { status, fileNamePrefix, notificationContact = DEFAULT_CONTACT } = config;
    const contact = notificationContact;
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `${fileNamePrefix}_${timestamp}.csv`;
    const filePath = path.join(OUTPUT_DIR, filename);

    try {
        await safeSendNotification(`Starting download for ${status} subscribers`, contact);

        // Initialize CSV file
        const initialRow = 'firstname,lastname,dateOfBirth,country,documentType,documentNumber,expiryDate,dateOfIssue,sex,state,updated,msisdn,naturalSubscriber,juristicSubscriber,registrationNumber,verificationReason,verificationState';
        await writeToFile(filePath,  initialRow)

        // Fetch initial data to get pagination details
        const initialResponse = await withRetry(() =>
            apiClient.get(`${BASE_URL}/${status}${DATE_RANGE}?size=20`)
            // apiClient.get(`${BASE_URL}/${status}${DATE_RANGE}?size=2000`)
        );

        const totalPages = 3;
        // const totalPages = initialResponse.data.pages;
        console.log(`Processing ${totalPages} pages of ${status} subscribers`);

        // Process all pages
        for (let currentPage = 0; currentPage < totalPages; currentPage++) {
            try {
                console.log(`Processing page ${currentPage + 1}/${totalPages}`);
                const pageData = await withRetry(() =>
                    // apiClient.get(`${BASE_URL}/${status}${DATE_RANGE}?size=20&page=${currentPage}`)
                    apiClient.get(`${BASE_URL}/${status}${DATE_RANGE}?size=2000&page=${currentPage}`)
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
                    writeToFile(filePath, row);
                })

            } catch (pageError) {
                console.error(`Error processing page ${currentPage}:`, pageError);
                await safeSendNotification(`Error processing page ${currentPage} for ${status} subscribers`, contact);
            }
        }

        await safeSendNotification(`Successfully downloaded ${status} subscribers`, contact);
        console.log(`Completed download for ${status} subscribers at: ${filePath}`);

    } catch (error) {
        console.error(`Critical error processing ${status} subscribers:`, error);
        await safeSendNotification(`Failed to download ${status} subscribers - contact support`, contact);
        throw error;
    } finally {
        // if (fileHandle) {
        //     await fileHandle.close();
        // }
    }
}

export const downloadAllFailedNaturalSubscribers = async (notificationContact) => {
    return downloadSubscribers({
        status: 'FAILED_VERIFICATION',
        fileNamePrefix: 'failed_natural-subscriber',
        notificationContact
    });
}

export const downloadAllVerifiedNaturalSubscribers = async (notificationContact) => {
    return downloadSubscribers({
        status: 'VERIFIED',
        fileNamePrefix: 'verified_natural-subscriber',
        notificationContact
    });
}

export const downloadAllPendingNaturalSubscribers = async (notificationContact) => {
    return downloadSubscribers({
        status: 'PENDING_VERIFICATION',
        fileNamePrefix: 'pending_natural-subscriber',
        notificationContact
    });
}


const statusToDownload= ['FAILED_VERIFICATION', 'PENDING_VERIFICATION','VERIFIED' ]
statusToDownload.map( async (status) => {
    await downloadSubscribers({
        status: status,
        fileNamePrefix: status.toLowerCase()+'_natural-subscriber',
        notificationContact:"+26773001762"
    });
})


//transfer to BDAP

const BDAP_CONFIG = {
    host: process.env.BDAP_HOST,          // BDAP server hostname
    port: parseInt(process.env.BDAP_PORT) || 22,  // SSH port (default: 22)
    username: process.env.BDAP_USERNAME,  // SSH username
    password: process.env.BDAP_PASSWORD,  // SSH password
    remoteDir: process.env.BDAP_REMOTE_DIR  // Remote directory for file storage
};

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






