# BOCRA SIMS API Integration Server

## Overview

This service fetches subscriber data from BOCRA's (Botswana Communications Regulatory Authority) SIMS API and uploads it to a BDAP (Botswana Data Analytics Platform) server. The system is designed to handle both natural and juristic subscribers, running on a weekly schedule to ensure data is kept up-to-date.

## System Architecture

The application is a Node.js server that communicates with the BOCRA SIMS API to fetch subscriber data, processes it into standardized formats, and securely transfers it to a BDAP server.

### Key Components

1. **API Client**: Custom Axios-based HTTP client for communicating with the BOCRA SIMS API
2. **Data Processing Pipeline**: Logic to extract, transform, and load subscriber data
3. **File Management**: Date-partitioned directory system for storing CSV exports
4. **BDAP Transfer**: Secure SCP-based file transfer to the BDAP server
5. **Scheduling**: Weekly cron jobs for automated data synchronization

## Data Flow Logic

### 1. Natural Subscribers

The system fetches natural subscribers in three verification states:
- `VERIFIED`: Successfully verified subscribers
- `FAILED_VERIFICATION`: Subscribers that failed verification
- `PENDING_VERIFICATION`: Subscribers awaiting verification

For each state, the system:
1. Creates a CSV file with appropriate headers
2. Fetches paginated data from the API
3. Processes each subscriber record, extracting phone numbers and documents
4. Writes standardized records to the CSV
5. Uploads the completed file to the BDAP server

### 2. Juristic Subscribers

For juristic (business) subscribers:
1. Fetches all juristic subscribers
2. For each subscriber:
   - Retrieves detailed information including contact persons
   - Fetches associated phone numbers
   - Consolidates data into standardized records
3. Writes records to a single CSV file
4. Uploads the file to the BDAP server

## Configuration

The system uses environment variables for configuration:

```
BDAP_HOST=<hostname>
BDAP_PORT=<port>
BDAP_USERNAME=<username>
BDAP_PASSWORD=<password>
BDAP_REMOTE_DIR=<remote_directory>
```

Additional constants in the code define:
- API endpoints and authentication
- Date ranges for historical data
- Retry mechanisms and timeouts
- Output directory structure

## Running the System

The system can be run in several modes:

### Standard Test Mode (Limited Data)
```
node Server.js
```
This runs a limited test with a small number of pages for development and testing.

### Production Mode (Full Data)
```
node Server.js --production
```
This runs a full data download without page limits, suitable for production use.

### Scheduled Weekly Download

The system automatically schedules a weekly full data download every Monday at 1 AM Botswana time.

### One-Time Push

The `scheduleOnePushToBDAP()` function can be used to trigger an immediate push of all subscriber data.

## Key Functions

- `downloadNaturalSubscribers`: Fetches natural subscribers for a specific verification state
- `downloadJuristicSubscribers`: Fetches all juristic subscribers
- `uploadToBDAP`: Securely transfers files to the BDAP server
- `scheduleWeeklyDownload`: Sets up the weekly cron job
- `testAPIConnectivity`: Verifies API connectivity
- `testBDAPConnection`: Verifies BDAP server connectivity

## Data Comparison Logic

The system implements several approaches to fetch and compare data:

1. **API Endpoint Variations**: The code tries multiple endpoint structures when fetching data, adapting to potential API changes

2. **Response Structure Handling**: The system can parse multiple response structures:
   - Standard paginated responses with `.content` array
   - Direct arrays of subscribers
   - Data wrapped in `.data` field
   - Custom `.subscribers` field

3. **Field Normalization**: Subscriber data is normalized into a consistent format:
   - Standardized field names
   - Common data types
   - Cleaned text (removing commas, line breaks)

4. **Alternative Fetching Methods**:
   - Primary method: Direct API calls to verification endpoints
   - Fallback method: MSISDN range scanning for natural subscribers
   - Different approach for juristic subscribers

5. **Verification State Mapping**:
   ```
   'ACTIVE' -> 'VERIFIED'
   'FAILED' -> 'FAILED_VERIFICATION'
   'PENDING' -> 'PENDING_VERIFICATION'
   ```

## Error Handling and Resilience

The system includes robust error handling:

1. **Retry Mechanisms**: Failed requests are retried up to a configurable number of times
2. **Connection Testing**: BDAP connection is tested before attempting file transfers
3. **Graceful Degradation**: When primary endpoints fail, alternative approaches are tried
4. **Detailed Logging**: Comprehensive logging for troubleshooting
5. **Notifications**: Optional notifications for critical events

## CSV File Structure

### Natural Subscribers
```
firstname,lastname,dateOfBirth,country,documentType,documentNumber,expiryDate,dateOfIssue,sex,state,updated,msisdn,naturalSubscriber,juristicSubscriber,registrationNumber,verificationReason,verificationState
```

### Juristic Subscribers
```
msisdn,subscriber_id,subscriber_type,number_state,subscriber_verification_state,subscriber_verification_reason,first_name,last_name,date_of_birth,registration_date,registration_number,country,sex,document_type,document_number,document_issue_date,document_expiry_date,address_plot,address_city,created_date,updated_date
```

## Security Considerations

- HTTPS connections with optional SSL verification
- Secure SCP file transfers
- Password-based authentication
- Connection timeouts and limits

## Development and Debugging

The system includes extensive debug logging and structured error handling to facilitate troubleshooting and development. API requests and responses are logged with detailed information about data structures and processing steps.



