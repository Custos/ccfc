# CCFC Voter Data Platform

A scalable, individual-centric voter data platform for processing, deduplicating, and normalizing voter data from multiple sources.

## Features

- Normalized schema design for voter data
- Entity resolution and deduplication
- Modular adapter system for different data sources
- Efficient processing of large datasets using chunking and multiprocessing
- Pipeline-based architecture for extensibility
- Database integration with SQLite and Google BigQuery

## Installation

1. Clone the repository:
   ```bash
   git clone [repository URL]
   cd ccfc
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install BigQuery dependencies (optional, for BigQuery support):
   ```bash
   pip install google-cloud-bigquery pandas-gbq pyarrow fsspec
   ```

## Data Directory Structure

The system expects the following directory structure:

```
ccfc/
├── config/
│   └── config.yaml
├── data/
│   ├── raw/
│   │   ├── rockland_voters.csv
│   │   └── putnam_voters.csv
│   └── processed/
├── schemas/
│   ├── core_voter_schema.json
│   ├── addresses_schema.json
│   ├── voter_registrations_schema.json
│   ├── election_history_schema.json
│   ├── donation_history_schema.json
│   ├── candidates_schema.json
│   └── committees_schema.json
├── src/
│   ├── adapters/
│   ├── matchers/
│   ├── processors/
│   ├── utils/
│   └── pipeline/
└── process_voter_data.py
```

## Usage

### Processing Voter Data

To process voter data:

```bash
python process_voter_data.py --config config.yaml --sources rockland,putnam
```

Options:
- `--config`: Path to a custom config file (default: config/config.yaml)
- `--sources`: Comma-separated list of sources to process (default: rockland,putnam)
- `--import-only`: Skip processing and only import existing processed data to a database
- `--db-type`: Specify database type to use ("sqlite" or "bigquery")

### Database Integration

The platform supports two database backends:

#### SQLite Integration

To process data and store it in SQLite:

```bash
python process_voter_data.py --config config.yaml --sources rockland,putnam --db-type sqlite
```

To import existing processed data into SQLite without reprocessing:

```bash
python process_voter_data.py --config config.yaml --import-only --db-type sqlite
```

#### BigQuery Integration

To process data and store it in BigQuery:

```bash
python process_voter_data.py --config config.yaml --sources rockland,putnam --db-type bigquery
```

To import existing processed data into BigQuery without reprocessing:

```bash
python process_voter_data.py --config config.yaml --import-only --db-type bigquery
```

## Setting Up Google Cloud & BigQuery

### 1. Create a Google Cloud Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Click on the project dropdown at the top of the page
3. Click "New Project"
4. Enter a project name and click "Create"
5. Note your Project ID (different from the name)

### 2. Enable the BigQuery API

1. In the Google Cloud Console, go to "APIs & Services" > "Library"
2. Search for "BigQuery API"
3. Click on "BigQuery API" in the results
4. Click "Enable"

### 3. Create a Service Account and Key

Using the Google Cloud Console:

1. Go to "IAM & Admin" > "Service Accounts"
2. Click "Create Service Account"
3. Enter a name (e.g., "ccfc-data-processor") and description
4. Click "Create and Continue"
5. Add the following roles:
   - BigQuery Data Editor
   - BigQuery Job User
6. Click "Continue" and then "Done"
7. Find your new service account in the list and click on it
8. Go to the "Keys" tab
9. Click "Add Key" > "Create new key"
10. Choose JSON format and click "Create"
11. The key file will be downloaded to your computer

Using the command line:

```bash
# Set your project ID
PROJECT_ID="your-project-id"

# Create service account
gcloud iam service-accounts create ccfc-data-processor \
  --display-name="CCFC Data Processor"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:ccfc-data-processor@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:ccfc-data-processor@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Create and download key file
gcloud iam service-accounts keys create ./caitfc-key.json \
  --iam-account=ccfc-data-processor@$PROJECT_ID.iam.gserviceaccount.com
```

### 4. Update Config Files

Update your `config.yaml` file with the following:

```yaml
database:
  type: "bigquery"  # Set to "bigquery" to use BigQuery
  bigquery:
    dataset_id: "ccfc"  # Name of the BigQuery dataset
    project_id: "your-project-id"  # Your Google Cloud Project ID
    location: "us"  # BigQuery dataset location
    service_account_key: "/path/to/your-key.json"  # Path to your service account key file
```

## Adding New Data Sources

1. Define the source in `config/config.yaml`
2. Create an adapter in `src/adapters/` that inherits from `BaseAdapter`
3. Implement the required methods for processing the source

## Entity Resolution

The system uses a multi-stage entity matching approach:

1. Exact matching on high-confidence fields
2. Probabilistic matching using multiple fields
3. Assignment of persistent IDs for each unique individual

## Output Files

The system generates the following output files in the `data/processed/` directory:

- `core_voters.csv`: Deduplicated voter records
- `addresses.csv`: Normalized address information
- `voter_registrations.csv`: County-specific registration records
- `election_history.csv`: Voting history linked to core voters

## Security Considerations

When using BigQuery:

1. **Service Account Keys**: Store service account key files securely and never commit them to version control
2. **Permissions**: Use the principle of least privilege - only grant necessary permissions to service accounts
3. **Access Control**: Use Google Cloud IAM to control who can access your BigQuery datasets and tables 