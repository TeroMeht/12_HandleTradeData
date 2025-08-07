import psycopg2
from configparser import ConfigParser
import json

# ✅ Database config function
def db_config(filename="database.ini", section="postgresql"):
    """
    Parse the database.ini file and return connection parameters.
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            f"Section {section} not found in the {filename} file."
        )
    return db

# ✅ Function to connect to the database
def connect_db():
    """
    Connect to the PostgreSQL database using parameters from db_config().
    Returns a psycopg2 connection object or None if connection fails.
    """
    connection = None
    try:
        params = db_config()
        connection = psycopg2.connect(**params)
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None

# ✅ Optional: Project-level JSON config
def read_project_config(config_file):
    """
    Read a JSON project configuration file and return as dict.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config
