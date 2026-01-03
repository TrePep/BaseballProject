import os
import requests
import re
import time
import pandas as pd
import zipfile
import io
import shutil
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Directories
DATA_DIR = 'Data'
MILB_RAW_DIR = os.path.join(DATA_DIR, 'MiLB')
MILB_PROCESSED_DIR = os.path.join(DATA_DIR, 'MiLB_Yearly')
NPB_RAW_DIR = os.path.join(DATA_DIR, 'NPB')
NPB_PROCESSED_DIR = os.path.join(DATA_DIR, 'NPB_Yearly')

# URLs
LAHMAN_URL = "https://github.com/chadwickbureau/baseballdatabank/archive/refs/tags/v2023.1.zip"
MILB_RELEASE_URL = "https://github.com/armstjc/milb-data-repository/releases/expanded_assets/game_player_stats"
NPB_BATTING_URL_PATTERN = "https://proeyekyuu.com/wp-content/CsvExports/PlayerSLBattingEN/player_batting_stats_en_{year}.csv"
NPB_PITCHING_URL_PATTERN = "https://proeyekyuu.com/wp-content/CsvExports/PlayerSLPitchingEN/player_pitching_stats_en_{year}.csv"

def get_engine(schema=None):
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        print("Error: Database configuration missing in .env file")
        return None
    
    base_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
    if schema:
        return create_engine(f"{base_string}/{schema}")
    return create_engine(base_string)

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_file(url, dest_path):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://proeyekyuu.com/csvs/'
        }
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        else:
            print(f"Failed to download (Status {response.status_code}): {url}")
            return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

# Download Lahman Data
def download_lahman_data():
    print("\n--- Downloading Lahman's Data ---")
    ensure_dir(DATA_DIR)
    
    # Check if files already exist
    if os.path.exists(os.path.join(DATA_DIR, 'People.csv')) and os.path.exists(os.path.join(DATA_DIR, 'Appearances.csv')):
        return

    try:
        response = requests.get(LAHMAN_URL)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for file_info in z.infolist():
                if file_info.filename.endswith('core/People.csv'):
                    print("Extracting People.csv...")
                    with z.open(file_info) as source, open(os.path.join(DATA_DIR, 'People.csv'), 'wb') as target:
                        shutil.copyfileobj(source, target)
                elif file_info.filename.endswith('core/Appearances.csv'):
                    print("Extracting Appearances.csv...")
                    with z.open(file_info) as source, open(os.path.join(DATA_DIR, 'Appearances.csv'), 'wb') as target:
                        shutil.copyfileobj(source, target)
    except Exception as e:
        print(f"Error downloading Lahman data: {e}")

# Download MiLB Data
def get_milb_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        pattern = r'href="(/armstjc/milb-data-repository/releases/download/game_player_stats/[^"]+\.csv)"'
        links = re.findall(pattern, response.text)
        return list(set([f"https://github.com{link}" for link in links]))
    except Exception as e:
        print(f"Error fetching MiLB page: {e}")
        return []

def download_milb_data():
    ensure_dir(MILB_RAW_DIR)
    
    links = get_milb_links(MILB_RELEASE_URL)
    
    for i, link in enumerate(links):
        filename = link.split("/")[-1]
        filepath = os.path.join(MILB_RAW_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"[{i+1}/{len(links)}] Downloading {filename}...")
            download_file(link, filepath)
            time.sleep(0.5)
        else:
            pass

# Download NPB Data
def download_npb_data():
    ensure_dir(NPB_RAW_DIR)
    
    start_year = 1950
    end_year = 2025
    
    for year in range(start_year, end_year + 1):
        batting_file = os.path.join(NPB_RAW_DIR, f"npb_batting_{year}.csv")
        if not os.path.exists(batting_file):
            download_file(NPB_BATTING_URL_PATTERN.format(year=year), batting_file)
            time.sleep(1)
            
        pitching_file = os.path.join(NPB_RAW_DIR, f"npb_pitching_{year}.csv")
        if not os.path.exists(pitching_file):
            download_file(NPB_PITCHING_URL_PATTERN.format(year=year), pitching_file)
            time.sleep(1)

# Process Data
def process_milb_data():
    ensure_dir(MILB_PROCESSED_DIR)
    
    filename_pattern = re.compile(r'(\d{4})_(\d{1,2})_([a-zA-Z0-9\+\-]+)_player_game_stats\.csv')
    file_groups = {}

    for filename in os.listdir(MILB_RAW_DIR):
        if filename.endswith("_player_game_stats.csv"):
            match = filename_pattern.match(filename)
            if match:
                season, month, level_code = match.groups()
                key = (season, level_code)
                if key not in file_groups:
                    file_groups[key] = []
                file_groups[key].append(os.path.join(MILB_RAW_DIR, filename))

    for (season, level_code), file_paths in file_groups.items():
        output_filename = f"MiLB_{season}_{level_code}.csv"
        output_path = os.path.join(MILB_PROCESSED_DIR, output_filename)
        
        if os.path.exists(output_path):
            continue

        dfs = []
        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path, usecols=['player_full_name', 'league_level_name', 'team_name', 'team_org_name'])
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df['Season'] = season
            combined_df.rename(columns={
                'player_full_name': 'Player Name',
                'league_level_name': 'League Level',
                'team_name': 'Team Name',
                'team_org_name': 'Team Org'
            }, inplace=True)
            
            result_df = combined_df.groupby(['Season', 'Player Name', 'League Level'], as_index=False).agg({
                'Team Name': lambda x: ', '.join(sorted(set(x.dropna().astype(str)))),
                'Team Org': lambda x: ', '.join(sorted(set(x.dropna().astype(str))))
            })
            result_df.sort_values(by=['Player Name'], inplace=True)
            result_df.to_csv(output_path, index=False)

def clean_npb_name(html_name):
    if not isinstance(html_name, str): return ""
    name_text = html_name
    match = re.search(r'>([^<]+)<', html_name)
    if match: name_text = match.group(1)
    if ',' in name_text:
        parts = name_text.split(',')
        if len(parts) >= 2: return f"{parts[1].strip()} {parts[0].strip()}"
    return name_text.strip()

def process_npb_data():
    ensure_dir(NPB_PROCESSED_DIR)
    
    # Find all years
    years = set()
    for f in os.listdir(NPB_RAW_DIR):
        match = re.search(r'_(\d{4})\.csv', f)
        if match: years.add(int(match.group(1)))
    
    for year in sorted(years):
        output_file = os.path.join(NPB_PROCESSED_DIR, f"NPB_{year}.csv")
        if os.path.exists(output_file):
            continue

        dfs = []
        batting_file = os.path.join(NPB_RAW_DIR, f"npb_batting_{year}.csv")
        pitching_file = os.path.join(NPB_RAW_DIR, f"npb_pitching_{year}.csv")
        
        if os.path.exists(batting_file):
            try:
                df = pd.read_csv(batting_file)
                if 'Name' in df.columns and 'Team' in df.columns: dfs.append(df[['Season', 'Name', 'Team']])
            except: pass
        
        if os.path.exists(pitching_file):
            try:
                df = pd.read_csv(pitching_file)
                if 'Name' in df.columns and 'Team' in df.columns: dfs.append(df[['Season', 'Name', 'Team']])
            except: pass
            
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            combined['Player Name'] = combined['Name'].apply(clean_npb_name)
            combined['League Level'] = 'NPB'
            combined['Team Org'] = combined['Team']
            combined.rename(columns={'Team': 'Team Name'}, inplace=True)
            
            final_df = combined[['Season', 'Player Name', 'League Level', 'Team Name', 'Team Org']]
            final_df = final_df.groupby(['Season', 'Player Name', 'League Level'], as_index=False).agg({
                'Team Name': lambda x: ', '.join(sorted(set(x.dropna().astype(str)))),
                'Team Org': lambda x: ', '.join(sorted(set(x.dropna().astype(str))))
            })
            final_df.sort_values(by='Player Name', inplace=True)
            final_df.to_csv(output_file, index=False)

# Load to MySQL
def load_csv_to_table(file_path, table_name, engine, add_id=False):
    if not os.path.exists(file_path): return
    
    try:
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='latin1')
            
        # Normalize columns for MiLB/NPB tables
        if 'Season' in df.columns:
            df.rename(columns={
                'Season': 'season',
                'Player Name': 'player_name',
                'League Level': 'league_level',
                'Team Name': 'team_name',
                'Team Org': 'team_org'
            }, inplace=True)
            
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
        if add_id:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `id` INT AUTO_INCREMENT PRIMARY KEY FIRST;"))
                conn.commit()
    except Exception as e:
        print(f"Error loading {table_name}: {e}")

def load_data_to_mysql():
    
    # Load Lahman Data (baseball_db)
    engine_main = get_engine('baseball_db')
    if engine_main:
        load_csv_to_table(os.path.join(DATA_DIR, 'People.csv'), 'people', engine_main)
        load_csv_to_table(os.path.join(DATA_DIR, 'Appearances.csv'), 'appearances', engine_main)
    
    # Load MiLB Data (milb schema)
    engine_milb = get_engine('milb')
    if engine_milb:
        files = [f for f in os.listdir(MILB_PROCESSED_DIR) if f.endswith('.csv')]
        for f in files:
            table_name = os.path.splitext(f)[0].lower()
            load_csv_to_table(os.path.join(MILB_PROCESSED_DIR, f), table_name, engine_milb, add_id=True)
            
    # Load NPB Data (npb schema)
    engine_npb = get_engine('npb')
    if engine_npb:
        files = [f for f in os.listdir(NPB_PROCESSED_DIR) if f.endswith('.csv')]
        for f in files:
            table_name = os.path.splitext(f)[0].lower()
            load_csv_to_table(os.path.join(NPB_PROCESSED_DIR, f), table_name, engine_npb, add_id=True)


def main():
    download_lahman_data()
    download_milb_data()
    download_npb_data()
    process_milb_data()
    process_npb_data()
    load_data_to_mysql()
    print("\nPipeline Complete")

if __name__ == "__main__":
    main()
