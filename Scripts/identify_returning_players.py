import pandas as pd
import os
from sqlalchemy import create_engine, text
import db_config

DB_NAME = 'baseball_db'

def run_mysql_query(sql_file_path, year):
    engine = db_config.get_engine(DB_NAME)
        
    with open(sql_file_path, 'r') as file:
        sql_template = file.read()
        
    sql_query = sql_template.format(year=year)

    df_result = pd.read_sql(sql_query, con=engine)
        
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
        
    print(df_result)

    new_table_name = f'players_returning_after_gap_{year}'
    print(f"\nSaving results to table 'yearly_results.{new_table_name}'...")
    df_result.to_sql(new_table_name, con=engine, schema='yearly_results', if_exists='replace', index=False)

    # Add the extra column for manual details and a Primary Key
    with engine.connect() as connection:
        try:
            # Add gap_details column
            connection.execute(text(f"ALTER TABLE yearly_results.{new_table_name} ADD COLUMN gap_details TEXT;"))
            
            # Add an auto-incrementing Primary Key 'id' at the start of the table
            connection.execute(text(f"ALTER TABLE yearly_results.{new_table_name} ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;"))
            
            print(f"Added 'gap_details' and Primary Key to {new_table_name}.")
        except Exception as e:
            print(f"Could not alter table {new_table_name}: {e}")

    print("Done.")

def run_summary_query(start_year, end_year):
    engine = db_config.get_engine(DB_NAME)
    
    print("\nGenerating summary table...")
    
    # Dynamically generate the UNION ALL query for the specified range
    union_parts = []
    for year in range(start_year, end_year):
        union_parts.append(f"SELECT {year} as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_{year}")
    
    sql_query = " UNION ALL ".join(union_parts) + " ORDER BY year;"
    
    df_result = pd.read_sql(sql_query, con=engine)
    print(df_result)
    
    table_name = 'returning_player_counts'
    df_result.to_sql(table_name, con=engine, schema='yearly_results', if_exists='replace', index=False)
        
if __name__ == "__main__":
    start_year = 1871
    end_year = 2025
    
    for year in range(start_year, end_year):
        print(f"Processing year: {year}")
        run_mysql_query('SQL/returning_players.sql', year)

    run_summary_query(start_year, end_year)