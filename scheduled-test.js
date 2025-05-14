import cron from 'node-cron';
import { downloadAllNaturalSubscribersProduction, downloadAllJuristicSubscribersProduction } from './Server.js';

console.log('Starting weekly scheduled job for Mondays at 5:00 AM...');

// Create a cron expression for 5:00 AM every Monday
// Format: second minute hour day month day-of-week
const cronExpression = '0 0 5 * * 1';

console.log(`Scheduling weekly run for: ${cronExpression} (5:00 AM every Monday)`);

// Schedule the weekly run
cron.schedule(cronExpression, async () => {
  console.log('WEEKLY DOWNLOAD TRIGGERED at:', new Date().toISOString());
  
  try {
    // Step 1: Run FULL natural subscribers download (all states)
    console.log('\n1. Running FULL natural subscribers download...');
    await downloadAllNaturalSubscribersProduction();
    console.log('✓ FULL natural subscribers download completed');
    
    // Step 2: Run FULL juristic subscribers download
    console.log('\n2. Running FULL juristic subscribers download...');
    await downloadAllJuristicSubscribersProduction();
    console.log('✓ FULL juristic subscribers download completed');
    
    console.log('\nWEEKLY DOWNLOAD COMPLETED SUCCESSFULLY');
    console.log('==========================================');
  } catch (error) {
    console.error('\n❌ WEEKLY DOWNLOAD FAILED:', error.message);
    console.error('Please check the logs for more details');
  }
}, {
  scheduled: true,
  timezone: "Africa/Gaborone" // Use Botswana timezone
});

// Keep the process alive
console.log('\nProcess will stay alive to maintain scheduled job...');
console.log('The weekly download will run every Monday at 5:00 AM.');
console.log('Press Ctrl+C to exit when done.'); 