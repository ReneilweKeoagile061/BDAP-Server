import { scheduleWeeklyDownload } from './Server.js';

console.log('Starting BOCRA API Scheduler...');
scheduleWeeklyDownload();
console.log('Scheduler is running. The job will execute every Sunday at 2 AM.'); 