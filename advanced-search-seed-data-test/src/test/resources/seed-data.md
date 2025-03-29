# MongoDB Atlas Local Seed Data Script

This directory contains scripts and data structures designed to seed a MongoDB Atlas Local container with sample data and configure Atlas Search indexes.

## Overview

The `seed-data.sh` script automatically:

1. Extracts compressed JSON data files (`.jsonl.gz`)
2. Imports the data into MongoDB collections 
3. Creates Atlas Search indexes for those collections

## Directory Structure

```
seed-data/
├── atlas-index-utils.js         # JavaScript utility functions for index operations
└── databases/
    └── examples/                # Database name
        └── collections/
            └── person/          # Collection name
                ├── atlas-search-indexes/
                │   └── person_search.json  # Search index configuration
                └── data/
                    └── documents.jsonl.gz  # Compressed data documents
```

## How It Works

1. **Data Import:**
   - The script navigates through the database directory structure
   - Uncompresses any `.jsonl.gz` files in the collection's data directory
   - Imports JSON documents into the corresponding database and collection
   
2. **Search Index Creation:**
   - Processes each JSON index definition found in `atlas-search-indexes` directories
   - Temporarily imports the index definition into a staging collection
   - Uses the `atlas-index-utils.js` MongoDB script to:
     - Drop any existing index with the same name
     - Create a new Atlas Search index
     - Wait for the index to be ready

## Atlas Search Index Example

The example includes a search index for the `person` collection that indexes:
- `name` (standard text search)
- `age` (numeric search)
- `job` (token and facet search)
- `bio` (standard text search)

## Usage

This directory is designed to be mounted as a volume in a MongoDB Atlas Local container and the script executed within that environment.