import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv(find_dotenv())

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def get_connection_string(schema=None):
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        raise ValueError("Database configuration missing in .env file")
    
    base_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
    
    if schema:
        return f"{base_string}/{schema}"
    return base_string

def get_engine(schema=None):
    conn_string = get_connection_string(schema)
    return create_engine(conn_string)
