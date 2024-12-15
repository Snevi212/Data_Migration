import pandas as pd
from sqlalchemy import create_engine

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "DB_connection",
    "port": 3306  # Default MySQL port
}

# MSSQL Configuration
MSSQL_CONFIG = {
    "host":"DESKTOP-BN99CMDSQLEXPRESS",
    "user":"root",
    "database":"Python",
    "password":"root",
    "driver": "ODBC Driver 17 for SQL Server"
}

def create_mysql_connection():
    """Create a connection to MySQL using SQLAlchemy."""
    try:
        mysql_engine = create_engine(
            f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
        )
        return mysql_engine
    except Exception as e:
        print("Error connecting to MySQL:", e)
        return None

def create_mssql_connection():
    """Create a connection to MSSQL using SQLAlchemy."""
    try:
        mssql_engine = create_engine(
            f"mssql+pyodbc://@{MSSQL_CONFIG['host']}/{MSSQL_CONFIG['database']}?driver={MSSQL_CONFIG['driver'].replace(' ', '+')}"
        )
        return mssql_engine
    except Exception as e:
        print("Error connecting to MSSQL:", e)
        return None

def load_data_from_mysql_to_mssql(mysql_query, sbi_customer):
    """Load data from MySQL to MSSQL."""
    mysql_engine = None
    mssql_engine = None
    try:
        # Establish MySQL connection
        mysql_engine = create_mysql_connection()
        if mysql_engine is None:
            print("Failed to connect to MySQL.")
            return

        # Establish MSSQL connection
        mssql_engine = create_mssql_connection()
        if mssql_engine is None:
            print("Failed to connect to MSSQL.")
            return

        # Fetch data from MySQL
        print("Fetching data from MySQL...")
        #mysql_query ="select * from sbi_bank_customer"
        df = pd.read_sql(mysql_query, con=mysql_engine)
        print(f"Data fetched from MySQL ({len(df)} rows).")

        # Write data to MSSQL
        print(f"Writing data to MSSQL table '{sbi_customer}'...")
        df.to_sql(name=sbi_customer, con=mssql_engine, if_exists="replace", index=False)
        print("Data successfully written to MSSQL.")

    except Exception as e:
        print("An error occurred:", e)

    finally:
        # Close MySQL connection
        if mysql_engine:
            mysql_engine.dispose()
            print("MySQL connection closed.")

        # Close MSSQL connection
        if mssql_engine:
            mssql_engine.dispose()
            print("MSSQL connection closed.")

# Example Usage
if __name__ == "__main__":
    # MySQL query to fetch data
    mysql_query = "SELECT * FROM sbi_bank_customers"

    # Target table in MSSQL
    target_table = "sbi_customer"

    # Load data
    load_data_from_mysql_to_mssql(mysql_query, target_table)
