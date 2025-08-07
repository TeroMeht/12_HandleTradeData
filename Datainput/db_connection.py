import psycopg2
from db_config import config


# Function to connect to the database
def connect_db():
    """Connect to the PostgreSQL database and return the connection object."""
    connection = None
    try:
        # Get connection parameters from the config
        params = config()
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None
