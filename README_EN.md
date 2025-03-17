# Square Customer Data Management Tool

This is a Python toolkit for managing Square customer data, featuring customer data import functionality.

## Features

- **Customer Data Import**: Support batch import of customer data into Square system
  - Support CSV and Excel (xlsx/xls) format data files
  - Support customer group management for easy classification and batch operations
  - Automatic detection and handling of duplicate customer data
- **Progress Display**: Real-time progress bar display
  - Show overall progress and current batch progress
  - Real-time update of success/failure count statistics
- **Logging**: Detailed recording of all operations and results

## Requirements

- Python 3.6+
- Square API Access Token

## Installation

1. Clone or download this project locally
2. Enter the project directory and install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the configuration file template:
```bash
cp .env.example .env
```

2. Edit the `.env` file to configure your Square API access token:
```env
SQUARE_ACCESS_TOKEN=your_access_token_here
SQUARE_ENVIRONMENT=sandbox  # or production
```

## Usage

### Importing Customer Data

Support importing customer data from CSV or Excel files:

```python
from square_customer_import import SquareCustomerImport

# Initialize the import tool
importer = SquareCustomerImport()

# Import customer data from CSV format
importer.import_customers('customers.csv')

# Or import customer data from Excel format
importer.import_customers('customers.xlsx')
```

#### Data File Format Requirements

- CSV/Excel files must include the following columns:
  - `Customer name`: Customer's name (format: surname/given name or full name)
  - `Customer email`: Customer's email
  - `Customer phone number`: Customer's phone (optional, international code will be added automatically)

### Customer Group Management

You can specify customer groups during import for easier management:

```python
# Import customer data and specify group
importer.import_customers('customers.csv', group_name='VIP Customers')
```

## Progress Display

The tool displays detailed progress information during batch operations:

- Overall progress bar: Shows the overall task completion percentage
- Batch progress bar: Shows the processing progress of the current batch
- Real-time statistics: Shows success/failure count statistics
- Log output: Synchronously displays detailed operation logs

## Notes

- Test in sandbox environment before using in production
- Ensure important data is backed up
- Do not commit the `.env` file to version control
- For large batch imports, it's recommended to process in batches, with each batch not exceeding 1000 records

## License

[MIT License](LICENSE)