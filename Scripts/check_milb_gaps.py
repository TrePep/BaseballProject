import pandas as pd
from sqlalchemy import create_engine, text
import db_config

DB_NAME = 'yearly_results' 

def update_gap_details():
    engine = db_config.get_engine(DB_NAME)
    
    # Get list of yearly_results tables
    with engine.connect() as conn:
        tables_query = text("SHOW TABLES LIKE 'players_returning_after_gap_%'")
        result_tables = conn.execute(tables_query).fetchall()
        result_tables = [t[0] for t in result_tables]

    # 2. Get list of tables and organize by year
    with engine.connect() as conn:
        milb_tables_query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'milb'")
        milb_tables_raw = conn.execute(milb_tables_query).fetchall()
        milb_tables_raw = [t[0] for t in milb_tables_raw]

    # Organize tables by year: {2007: ['milb_2007_aaa', 'milb_2007_aa', ...], ...}
    milb_tables_by_year = {}
    for t in milb_tables_raw: # Format: milb_YYYY_level
        parts = t.split('_')
        if len(parts) >= 2 and parts[1].isdigit():
            year = int(parts[1])
            if year not in milb_tables_by_year:
                milb_tables_by_year[year] = []
            milb_tables_by_year[year].append(f"milb.`{t}`")

    # Iterate through yearly_results tables
    for table in result_tables:
        try:
            table_year = int(table.split('_')[-1])
        except:
            continue
        
        df = pd.read_sql_table(table, engine)
        
        if df.empty:
            continue

        updates_made = 0
        
        for index, row in df.iterrows():
            player_id = row.get('id') # Primary key
            if not player_id:
                continue
                
            first_name = row['nameFirst']
            last_name = row['nameLast']
            full_name = f"{first_name} {last_name}"
            
            last_seen = row['last_seen_year']
            return_year = row['return_year']
            
            gap_years = range(last_seen + 1, return_year)
            
            gap_details_list = []
            found_match = False
            
            def normalize_name(n): # Remove periods, spaces, and lowercase
                if not n: return ""
                return n.replace('.', '').replace(' ', '').lower()

            normalized_full_name = normalize_name(full_name)

            for y in gap_years:
                if y in milb_tables_by_year:
                    for milb_table in milb_tables_by_year[y]:
                        # Use LIKE to find potential matches, then verify with normalization in Python
                        safe_first = first_name.replace("'", "''").split(' ')[0] 
                        
                        safe_last = last_name.replace("'", "''")
                        
                        detail_sql = text(f"SELECT player_name, league_level, team_name, team_org FROM {milb_table} WHERE player_name LIKE '%{safe_last}'")
                        
                        with engine.connect() as conn:
                            results = conn.execute(detail_sql).fetchall()
                            
                        for res in results:
                            milb_player_name = res[0]
                            if normalize_name(milb_player_name) == normalized_full_name:
                                found_match = True
                                level = res[1]
                                team = res[2]
                                org = res[3]
                                gap_details_list.append(f"{y} - {level}, {team}, {org}") # Format: 2007 - AAA, Richmond Braves, Atlanta Braves
            
            if found_match:
                new_details = "; ".join(gap_details_list)

                safe_details = new_details.replace("'", "''")
                
                update_sql = text(f"UPDATE {table} SET gap_details = '{safe_details}' WHERE id = {player_id}")
                with engine.connect() as conn:
                    conn.execute(update_sql)
                    conn.commit()
                updates_made += 1

if __name__ == "__main__":
    update_gap_details()
