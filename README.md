# CCFC Voter Data Platform

A scalable, individual-centric voter data platform for processing, deduplicating, and normalizing voter data from multiple sources.

## Features

- Normalized schema design for voter data
- Entity resolution and deduplication
- Modular adapter system for different data sources
- Efficient processing of large datasets using chunking and multiprocessing
- Pipeline-based architecture for extensibility

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

To process voter data:

```bash
./process_voter_data.py --sources rockland,putnam
```

Options:
- `--config`: Path to a custom config file (default: config/config.yaml)
- `--sources`: Comma-separated list of sources to process (default: rockland,putnam)

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