import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
import { downloadSubscribers } from './Server.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Set environment variables for BDAP server
process.env.BDAP_HOST = '10.1.31.209';
process.env.BDAP_USER = 'dev_team';
process.env.BDAP_TARGET_DIR = '/Upload/BDAP/BDAP/bocra_numbers';

// Test different API key formats
const apiKeyVariations = [
    {
        name: "Original",
        key: "BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA="
    },
    {
        name: "Base64 Encoded",
        key: Buffer.from("BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA=").toString('base64')
    },
    {
        name: "URL Encoded",
        key: encodeURIComponent("BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA=")
    },
    {
        name: "Without Base64 Padding",
        key: "BC.bUk37gtNG5w9T422BYAxHrlXITP32iFi.LuAZLfAhCDL4V235eXdhbKNzI/QXvDh7fTO5W8a/NZA"
    }
];

console.log('Testing BOCRA API Integration...');
try {
    await downloadSubscribers();
    console.log('Test completed successfully');
} catch (error) {
    console.error('Test failed:', error);
} 