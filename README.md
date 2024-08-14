# Database Object Extraction (DOS)

## Overview

Python script which connects to a remote database using a provided connection URI, extracts various database objects (such as tables, views, functions, procedures, and indexes), and saves these objects as SQL files.It supports the following databases:

- **Microsoft SQL Server**
- **Azure SQL Server**
- **Azure Synapse**

## Usage

### Command-Line Arguments

The script accepts the following command-line arguments:

1. `<remote>`: A remote identifier for the database, used as part of the output path for saving the SQL files.
2. `<database_uri>`: The database connection URI in the format `user:password@host/databaseName`.

### Example

To run the script, use the following command:

```bash
python dos.py <remote> <database_uri>