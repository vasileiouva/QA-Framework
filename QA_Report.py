import pandas as pd
import logging
from sqlalchemy import create_engine
import configparser

# Function to initialize database connection
def initialize_database(config_path='config/config.ini'):
    """
    Reads configuration settings and initializes a database connection.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    db_config = config['Database']
    engine = create_engine(
        f'mssql+pymssql://{db_config["username"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}'
    )
    return engine

# Function to load data from files
def load_data_from_files(file_paths, encoding='utf-8'):
    """
    Reads raw data from file paths.
    """
    return {name: pd.read_csv(path, encoding=encoding) for name, path in file_paths.items()}

# Function to load data from database tables
def load_data_from_database(engine, queries):
    """
    Executes SQL queries to load data into DataFrames.
    """
    return {name: pd.read_sql(query, engine) for name, query in queries.items()}

# Function to clean column names
def clean_column_names(df):
    """
    Strips and standardizes column names by removing unwanted characters.
    """
    df.columns = df.columns.str.strip().str.replace('ï', '', regex=False).str.replace(r'[^\w\s/\-]', '', regex=True)
    return df

# Function to clean numeric columns
def clean_numeric_column(df, column_name):
    """
    Cleans and converts a column to numeric, handling invalid values.
    """
    df[column_name] = df[column_name].astype(str).str.replace(',', '').str.strip().replace({'-': 0, 'nan': 0})
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0).astype(float)
    return df

# Function to drop unnecessary columns
def drop_unnecessary_columns(dataframes, columns_to_drop):
    """
    Drops specified columns from a list of DataFrames.
    """
    for df in dataframes:
        df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Function to validate row counts
def validate_row_counts(raw_df, ingested_df, file_name):
    """
    Compares row counts between raw and ingested data.
    Logs discrepancies and provides examples of differences.
    """
    if raw_df.shape[0] == ingested_df.shape[0]:
        logging.info(f'Row count for {file_name} matches between raw and ingested data.')
    else:
        diff = raw_df.shape[0] - ingested_df.shape[0]
        logging.error(f'Row count mismatch for {file_name}. Difference: {diff}')
        raw_diff = raw_df[~raw_df.apply(tuple, 1).isin(ingested_df.apply(tuple, 1))]
        ingested_diff = ingested_df[~ingested_df.apply(tuple, 1).isin(raw_df.apply(tuple, 1))]
        logging.error(f'Example rows in raw but not in ingested for {file_name}: {raw_diff.head()}')
        logging.error(f'Example rows in ingested but not in raw for {file_name}: {ingested_diff.head()}')

# Function to validate column counts
def validate_column_counts(raw_df, ingested_df, file_name):
    """
    Compares column counts and names between raw and ingested data.
    Logs discrepancies.
    """
    if raw_df.shape[1] == ingested_df.shape[1]:
        raw_df.columns = ingested_df.columns
        logging.info(f'Column count for {file_name} matches between raw and ingested data.')
    else:
        logging.error(f'Column count mismatch for {file_name}.')
        logging.error(f'Raw columns: {raw_df.columns}')
        logging.error(f'Ingested columns: {ingested_df.columns}')

# Function to validate grouped data consistency
def validate_grouped_data(raw_df, ingested_df, file_name, group_field, numeric_fields, tol=1):
    """
    Compares grouped data consistency by a specified field.
    Logs discrepancies in row counts and sums of numeric fields.

    Args:
        raw_df (pd.DataFrame): Raw data DataFrame.
        ingested_df (pd.DataFrame): Ingested data DataFrame.
        file_name (str): Name of the file or dataset being validated.
        group_field (str): Field to group data by (e.g., date_field).
        numeric_fields (list): List of numeric fields to compare.
        tol (float): Tolerance for numeric differences. Default is 1 pound.
    """
    try:
        raw_grouped = raw_df.groupby(group_field).agg({field: 'sum' for field in numeric_fields})
        raw_grouped['row_count'] = raw_df.groupby(group_field).size()

        ingested_grouped = ingested_df.groupby(group_field).agg({field: 'sum' for field in numeric_fields})
        ingested_grouped['row_count'] = ingested_df.groupby(group_field).size()

        differences = raw_grouped.subtract(ingested_grouped, fill_value=0).abs()
        differences_exceeding_tol = differences[(differences > tol).any(axis=1)]

        if not differences_exceeding_tol.empty:
            logging.error(f'Grouped data mismatch for {file_name}. Differences exceeding tolerance: {differences_exceeding_tol}')
        else:
            logging.info(f'Grouped data for {file_name} matches within tolerance.')
    except Exception as e:
        logging.error(f'Error validating grouped data for {file_name}: {e}')

# Function to set up logging
def setup_logging():
    """
    Configures logging for the application.
    """
    logging.basicConfig(
        filename='logs/qa_report.log',
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Main function
if __name__ == "__main__":
    setup_logging()
    logging.info("Starting data quality checks...")

    # Initialize database connection
    engine = initialize_database()

    # Define file paths and database queries
    file_paths = {
        'budget': 'data/raw/budget.csv',
        'costs': 'data/raw/costs.csv',
        'crm': 'data/raw/crm.csv',
        'revenue': 'data/raw/revenue.csv',
        'arr': 'data/raw/arr.csv'
    }

    db_queries = {
        'budget': 'SELECT * FROM Budget',
        'costs': 'SELECT * FROM Cost',
        'crm': 'SELECT * FROM CRM',
        'revenue': 'SELECT * FROM Revenue',
        'arr': 'SELECT * FROM ARR_source',
        'jira': 'SELECT * FROM JiraData'
    }

    # Load data
    raw_data = load_data_from_files(file_paths, encoding='latin1')
    ingested_data = load_data_from_database(engine, db_queries)

    # Clean column names
    for df_name, df in raw_data.items():
        raw_data[df_name] = clean_column_names(df)

    for df_name, df in ingested_data.items():
        ingested_data[df_name] = clean_column_names(df)

    # Drop unnecessary columns (as the ingested data also contains some metadata columns)
    drop_unnecessary_columns(
        [ingested_data[name] for name in ingested_data if name in ['budget', 'costs', 'crm', 'revenue', 'arr']],
        ['id', 'createdAt', 'updatedAt']
    )

    # Validate row and column counts
    for name, raw_df in raw_data.items():
        ingested_df = ingested_data.get(name)
        if ingested_df is not None:
            validate_row_counts(raw_df, ingested_df, name)
            validate_column_counts(raw_df, ingested_df, name)

    # Validate grouped data consistency
    numeric_fields = {
        'revenue': ['Revenue'],
        'costs': ['Cost'],
        'arr': ['Revenue'],
    }

    for name, raw_df in raw_data.items():
        ingested_df = ingested_data.get(name)
        if name in numeric_fields and ingested_df is not None:
            validate_grouped_data(raw_df, ingested_df, name, 'date_field', numeric_fields[name], tol=1)

    logging.info("Data quality checks completed.")
    logging.shutdown()
