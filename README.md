# Baseball Gap Analysis Project

This project analyzes Major League Baseball (MLB) players who returned to the league after a gap of one or more years. It fills in these gap years with Minor League Baseball (MiLB) and Nippon Professional Baseball (NPB) data to determine if the player was active in one of those leagues during their absence from MLB.

**The results of this project are the tables found in the [Results](Results/) directory.**

## Prerequisites

*   **Python 3.8+**
*   **MySQL Server** 

## Setup

1.  **Clone the repository** to your local machine.
    ```bash
    git clone https://github.com/TrePep/BaseballProject
    cd BaseballProject
    ```

2.  **Install Python dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Database Configuration**:
    *   Modify the `.env` file in the project root (copy the structure below).
    *   Fill in your MySQL credentials.


4.  **Create MySQL Schemas**:
    Log in to your MySQL server and create the following schemas (databases):
    ```sql
    CREATE DATABASE baseball_db;
    CREATE DATABASE milb;
    CREATE DATABASE npb;
    CREATE DATABASE yearly_results;
    ```

## Running the Project

### 1. Data Setup (ETL Pipeline)

Run the main ETL pipeline script. This script handles:
*   Downloading Lahman's Baseball Data (if not present).
*   Downloading MiLB stats from the [MiLB Data Repository](https://github.com/armstjc/milb-data-repository).
*   Downloading NPB stats from [ProEyekyuu.com](https://proeyekyuu.com).
*   Processing the raw CSVs into standardized formats.
*   Loading all data into your MySQL databases (`baseball_db`, `milb`, `npb`).

```bash
python Scripts/etl_pipeline.py
```

### 2. Run Analysis

**Step 1: Identify Returning Players**
Run the query to find players who played in MLB, missed at least one season, and then returned. This creates tables in the `yearly_results` schema (e.g., `players_returning_after_gap_2005`).

```bash
python Scripts/identify_returning_players.py
```

**Step 2: Cross-Reference with MiLB**
Check if the players found in Step 1 were playing in the Minor Leagues during their gap years. This updates the `gap_details` column in the `yearly_results` tables.

```bash
python Scripts/check_milb_gaps.py
```

**Step 3: Cross-Reference with NPB**
Check if the players found in Step 1 were playing in Japan (NPB) during their gap years. This appends to the `gap_details` column.

```bash
python Scripts/check_npb_gaps.py
```

## Project Structure

*   `Data/`: Stores raw and processed CSV data.
*   `Scripts/`: Python scripts for ETL and analysis.
    *   `etl_pipeline.py`: Main script for downloading, processing, and loading data.
    *   `identify_returning_players.py`: Identifies players with gap years in MLB.
    *   `check_milb_gaps.py`: Checks for MiLB activity during gap years.
    *   `check_npb_gaps.py`: Checks for NPB activity during gap years.
    *   `db_config.py`: Database connection helper.
*   `SQL/`: SQL templates used by the analysis scripts.
*   `Results/`: Folder for exporting results to CSV.
