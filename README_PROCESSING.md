# Voter Data Processing Guide

This guide provides step-by-step instructions for processing voter data using the CCFC Voter Data Platform.

## Prerequisites

1. Python 3.8+ installed
2. Virtual environment created and activated
3. Required dependencies installed (`pip install -r requirements.txt`)
4. Raw data files placed in the `data/raw` directory

## Step 1: Validate the Data Files

Before processing, it's a good idea to validate the structure and contents of your data files.

```bash
# Validate Rockland voter data
python validate_data.py data/raw/rockland_voters.csv --type rockland

# Validate Putnam voter data
python validate_data.py data/raw/putnam_voters.csv --type putnam

# Validate donations data 
python validate_data.py data/raw/mondaire_donations.csv --type donations
```

This will check for required columns and provide a summary of the data file contents.

## Step 2: Process the Voter Data

Use the main processing script to process the voter data files:

```bash
# Process both Rockland and Putnam voter data
python process_voter_data.py --config config/config.yaml --sources rockland,putnam
```

This will:
1. Load the configuration from `config/config.yaml`
2. Initialize the pipeline controller
3. Process voter registration data from both sources
4. Match entities across data sources
5. Process and save the data into normalized tables

## Step 3: Check the Processed Data

After processing, the normalized data tables will be saved in the `data/processed` directory. You can check these files to verify that the processing completed successfully:

```bash
ls -la data/processed/
```

You should see the following files:
- `core_voters.csv`: Contains core voter information (name, DOB, etc.)
- `addresses.csv`: Contains address information
- `voter_registrations.csv`: Contains voter registration records
- `election_history.csv`: Contains election participation history

## Step 4: Explore the SQLite Database

The data is also loaded into a SQLite database at `data/ccfc.db`. You can explore it using the SQLite command-line tool:

```bash
sqlite3 data/ccfc.db
```

Inside the SQLite shell:

```sql
-- View all tables
.tables

-- Count records in each table
SELECT COUNT(*) FROM core_voters;
SELECT COUNT(*) FROM addresses;
SELECT COUNT(*) FROM voter_registrations;
SELECT COUNT(*) FROM election_history;

-- Example query: Find voters who voted in multiple elections
SELECT voter_id, COUNT(*) as election_count
FROM election_history
GROUP BY voter_id
HAVING election_count > 5
ORDER BY election_count DESC
LIMIT 10;
```

## Step 5: Prepare for BigQuery (Optional)

If you want to load the data into BigQuery:

1. Make sure you have the Google Cloud SDK installed and configured
2. Update `config.yaml` to use BigQuery as the database type
3. Run the following command to load the data into BigQuery:

```bash
# This assumes you've already processed the data locally
python load_to_bigquery.py --config config/config.yaml
```

## Troubleshooting

If you encounter any issues during processing:

1. **Check the logs**: The processor logs detailed information during execution
2. **Validate input data**: Make sure your input files match the expected format
3. **Check disk space**: Large voter files can require substantial disk space for processing
4. **Memory issues**: For very large files, you may need to increase the `chunk_size` in `config.yaml`

## Next Steps

After successfully processing the data, you can:

1. Perform analysis using the normalized data
2. Build visualizations and reports
3. Match against other data sources
4. Develop targeting models for voter outreach 