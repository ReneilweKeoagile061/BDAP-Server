import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();
console.log('Environment variables loaded from .env file');

// Define required environment variables
const requiredVars = [
  'BDAP_HOST',
  'BDAP_PORT',
  'BDAP_USERNAME',
  'BDAP_PASSWORD',
  'BDAP_REMOTE_DIR'
];

// Check for missing variables
const missingVars = requiredVars.filter(varName => !process.env[varName]);

// Display status
console.log('\nEnvironment Variable Check:');
console.log('=========================');

if (missingVars.length === 0) {
  console.log('✅ All required environment variables are defined!');
} else {
  console.log('❌ Missing required environment variables:');
  missingVars.forEach(varName => {
    console.log(`   - ${varName}`);
  });
  console.log('\nPlease create or update your .env file with these variables.');
}

// Display current configuration (without showing actual passwords)
console.log('\nCurrent Configuration:');
console.log('====================');
requiredVars.forEach(varName => {
  if (varName === 'BDAP_PASSWORD' && process.env[varName]) {
    console.log(`${varName}: [HIDDEN]`);
  } else if (process.env[varName]) {
    console.log(`${varName}: ${process.env[varName]}`);
  } else {
    console.log(`${varName}: [NOT SET]`);
  }
});

console.log('\nOther Environment Details:');
console.log('========================');
console.log(`Node.js Version: ${process.version}`);
console.log(`Platform: ${process.platform}`);
console.log(`Architecture: ${process.arch}`);
console.log(`Current Working Directory: ${process.cwd()}`); 