import { downloadSubscribers } from './Server.js';
import fs from 'fs/promises';
import path from 'path';

async function showDirectoryStructure(dir, prefix = '') {
    const items = await fs.readdir(dir, { withFileTypes: true });
    for (const item of items) {
        console.log(prefix + (item.isDirectory() ? '📁 ' : '📄 ') + item.name);
        if (item.isDirectory()) {
            await showDirectoryStructure(path.join(dir, item.name), prefix + '  ');
        }
    }
}

async function readLatestFile(outputDir) {
    const allFiles = [];
    
    // Recursively find all CSV files
    async function findFiles(dir) {
        const items = await fs.readdir(dir, { withFileTypes: true });
        for (const item of items) {
            const fullPath = path.join(dir, item.name);
            if (item.isDirectory()) {
                await findFiles(fullPath);
            } else if (item.name.endsWith('.csv')) {
                allFiles.push(fullPath);
            }
        }
    }
    
    await findFiles(outputDir);
    
    // Sort by modification time and get the latest
    const fileStats = await Promise.all(
        allFiles.map(async file => ({
            file,
            stat: await fs.stat(file)
        }))
    );
    
    const latestFile = fileStats
        .sort((a, b) => b.stat.mtime.getTime() - a.stat.mtime.getTime())[0];
    
    if (latestFile) {
        const content = await fs.readFile(latestFile.file, 'utf-8');
        return { path: latestFile.file, content };
    }
    return null;
}

async function runDemo() {
    console.log('\n🚀 Starting BOCRA API Integration Demo...\n');

    try {
        // Step 1: Run the data collection
        console.log('📥 Collecting subscriber data...');
        await downloadSubscribers();
        
        // Step 2: Show the directory structure
        console.log('\n📁 Generated directory structure:');
        await showDirectoryStructure('./output');
        
        // Step 3: Show the latest file contents
        console.log('\n📄 Latest generated file contents:');
        const latestFile = await readLatestFile('./output');
        if (latestFile) {
            console.log('\nFile path:', latestFile.path);
            console.log('\nFirst 5 lines of the file:');
            const lines = latestFile.content.split('\n');
            console.log(lines.slice(0, Math.min(6, lines.length)).join('\n'));
            
            // Count total records
            const recordCount = lines.length - 2; // Subtract header and empty last line
            console.log(`\nTotal records in file: ${recordCount}`);
        }
        
        console.log('\n✅ Demo completed successfully!');
        console.log('\nTo start the weekly scheduler (Sundays at 2 AM):');
        console.log('$ npm start');
        
    } catch (error) {
        console.error('\n❌ Demo failed:', error);
    }
}

// Run the demo
runDemo(); 