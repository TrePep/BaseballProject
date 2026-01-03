import pandas as pd
import os
from sqlalchemy import create_engine
import db_config

DB_NAME = 'baseball_db'
OUTPUT_DIR = 'Results'

def export_tables_to_csv():
    engine = db_config.get_engine(DB_NAME)
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

    for year in range(1871, 2025):
        table_name = f'players_returning_after_gap_{year}'
        full_table_name = f'yearly_results.{table_name}'
        
        print(f"Exporting {full_table_name}...")
        try:
            df = pd.read_sql(f"SELECT * FROM {full_table_name}", con=engine)
            
            output_file = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
            df.to_csv(output_file, index=False)
            print(f"Saved to {output_file}")
        except Exception as e:
            print(f"Error exporting {full_table_name}: {e}")

    summary_table = 'returning_player_counts'
    full_summary_name = f'yearly_results.{summary_table}'
    print(f"\nExporting {full_summary_name}...")
    try:
        df_summary = pd.read_sql(f"SELECT * FROM {full_summary_name}", con=engine)
        output_file = os.path.join(OUTPUT_DIR, f"{summary_table}.csv")
        df_summary.to_csv(output_file, index=False)

    except Exception as e:
        print(f"Error exporting {full_summary_name}: {e}")


if __name__ == "__main__":
    export_tables_to_csv()
