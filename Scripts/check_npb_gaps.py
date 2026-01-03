import pandas as pd
from sqlalchemy import create_engine, text
import db_config

DB_NAME = 'yearly_results' 

def update_gap_details_npb():
    engine = db_config.get_engine(DB_NAME)
    
    # Get list of yearly_results tables
    with engine.connect() as conn:
        tables_query = text("SHOW TABLES LIKE 'players_returning_after_gap_%'")
        result_tables = conn.execute(tables_query).fetchall()
        result_tables = [t[0] for t in result_tables]

    # Get list of npb tables and organize by year
    with engine.connect() as conn:
        npb_tables_query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'npb'")
        npb_tables_raw = conn.execute(npb_tables_query).fetchall()
        npb_tables_raw = [t[0] for t in npb_tables_raw]

    
    npb_tables_by_year = {}
    for t in npb_tables_raw: # Organize npb tables by year: {2007: ['npb.npb_2007'], ...}
        parts = t.split('_')
        if len(parts) >= 2 and parts[1].isdigit():
            year = int(parts[1])
            if year not in npb_tables_by_year:
                npb_tables_by_year[year] = []
            npb_tables_by_year[year].append(f"npb.`{t}`")

    for table in result_tables:
        try:
            table_year = int(table.split('_')[-1])
        except:
            continue
        
        df = pd.read_sql_table(table, engine)
        
        if df.empty:
            continue

        updates_made = 0
        
        # Iterate rows
        for index, row in df.iterrows():
            player_id = row.get('id')
            if not player_id:
                continue
                
            first_name = row['nameFirst']
            last_name = row['nameLast']
            full_name = f"{first_name} {last_name}"
            
            last_seen = row['last_seen_year']
            return_year = row['return_year']
            
            gap_years = range(last_seen + 1, return_year)
            mlb_years_to_check = [last_seen, return_year] # Define MLB Years (to check for exclusion)
            
            def normalize_name(n):
                if not n: return ""
                return n.replace('.', '').replace(' ', '').lower()

            normalized_full_name = normalize_name(full_name)

            is_namesake = False
            for y in mlb_years_to_check:
                if y in npb_tables_by_year:
                    for npb_table in npb_tables_by_year[y]:
                        safe_last = last_name.replace("'", "''")
                        check_sql = text(f"SELECT player_name FROM {npb_table} WHERE player_name LIKE '%{safe_last}'")
                        with engine.connect() as conn:
                            results = conn.execute(check_sql).fetchall()
                            for res in results:
                                if normalize_name(res[0]) == normalized_full_name:
                                    is_namesake = True
                                    break
                if is_namesake:
                    break
            
            if is_namesake:
                continue

            gap_details_list = []
            found_match = False
            
            for y in gap_years:
                if y in npb_tables_by_year:
                    for npb_table in npb_tables_by_year[y]:
                        safe_last = last_name.replace("'", "''")
                        detail_sql = text(f"SELECT player_name, league_level, team_name FROM {npb_table} WHERE player_name LIKE '%{safe_last}'")
                        
                        with engine.connect() as conn:
                            results = conn.execute(detail_sql).fetchall()
                            
                        for res in results:
                            npb_player_name = res[0]
                            if normalize_name(npb_player_name) == normalized_full_name:
                                found_match = True
                                level = res[1]
                                team = res[2]
                                gap_details_list.append(f"{y} - {level}, {team}")
            
            if found_match:
                current_details = row.get('gap_details')
                if current_details:
                    new_details = current_details + "; " + "; ".join(gap_details_list)
                else:
                    new_details = "; ".join(gap_details_list)
                
                safe_details = new_details.replace("'", "''")
                
                update_sql = text(f"UPDATE {table} SET gap_details = '{safe_details}' WHERE id = {player_id}")
                with engine.connect() as conn:
                    conn.execute(update_sql)
                    conn.commit()
                updates_made += 1
                
        if updates_made > 0:
            print(f"  Updated {updates_made} players in {table}.")

if __name__ == "__main__":
    update_gap_details_npb()
