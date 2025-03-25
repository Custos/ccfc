# Processing FEC Donations

This guide explains how to process FEC donation data using the CCFC Voter Data Platform.

## Overview

The donation processing system:

1. Imports donation data from CSV files
2. Extracts and creates/updates committee records
3. Extracts and creates/updates candidate records
4. Matches donors to existing core voters or creates new voter records
5. Stores the donation history in the database

## Prerequisites

1. Python 3.8+ installed
2. Virtual environment created and activated
3. Required dependencies installed (`pip install -r requirements.txt`)
4. Raw donation data files placed in the `data/raw` directory

## Processing Mondaire Donations

To process the Mondaire donations file specifically:

```bash
python process_mondaire_donations.py
```

This script will automatically:
- Load the configuration from `config/config.yaml`
- Process the `data/raw/mondaire_donations.csv` file
- Create or update committees, candidates, and core voters
- Store donation records in the database

## Processing General FEC Donations

For other FEC donation files:

```bash
python -m src.scripts.process_donations --config config/config.yaml --files data/raw/your_donation_file.csv
```

You can process multiple files at once:

```bash
python -m src.scripts.process_donations --config config/config.yaml --files data/raw/file1.csv data/raw/file2.csv
```

## Data Schema

The donation processing uses the following schemas:

- `donation_history_schema.json`: Schema for donation records
- `committees_schema.json`: Schema for committee records
- `candidates_schema.json`: Schema for candidate records
- `core_voter_schema.json`: Schema for donor records (as core voters)

## Entity Matching

When processing donations, the system attempts to match donors to existing core voters using:
- Name (first, middle, last)
- Location (city, state, zip)

If a match is found, the donation is linked to the existing voter. If no match is found, a new voter record is created.

## Viewing the Processed Data

After processing, you can query the SQLite database to view the results:

```bash
sqlite3 data/ccfc.db
```

Example queries:

```sql
-- View all committees
SELECT committee_id, committee_name FROM committees;

-- View all candidates
SELECT candidate_id, candidate_name FROM candidates;

-- View donations by committee
SELECT committee_id, COUNT(*) as donation_count, SUM(contribution_receipt_amount) as total_amount
FROM donation_history
GROUP BY committee_id;

-- View donations by contributor
SELECT contributor_first_name, contributor_last_name, contributor_city, 
       COUNT(*) as donation_count, SUM(contribution_receipt_amount) as total_amount
FROM donation_history
WHERE is_individual = 1
GROUP BY contributor_first_name, contributor_last_name, contributor_city
ORDER BY total_amount DESC
LIMIT 20;
```

## Troubleshooting

If you encounter issues during processing:

1. **Check the logs**: The processor outputs detailed information during execution
2. **Validate input data**: Ensure your input files have the expected format with required columns
3. **Database access**: Make sure you have write permissions to the database file
4. **Memory issues**: For very large files, increase the `chunk_size` in `config.yaml` 