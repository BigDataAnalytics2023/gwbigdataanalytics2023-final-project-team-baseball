import json
import boto3
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import numpy as np


# logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def explode_list(df, lst_col):
   
    df[lst_col] = df[lst_col].apply(lambda x: x if isinstance(x, list) else [])    
    lens = df[lst_col].str.len()    
    mask = lens > 0

    
    if mask.any():
        res = pd.DataFrame({
            col: np.repeat(df[col][mask], lens[mask])
            for col in df.columns.drop(lst_col)}
        )
        res[lst_col] = np.concatenate(df[lst_col][mask])
    else:
        res = pd.DataFrame(columns=df.columns)  
    res_empty = df[~mask]
    res = pd.concat([res, res_empty], ignore_index=True)
    if res.empty:
        return df
    else:
        res = pd.concat([res, res_empty], ignore_index=True)
    return res

def extract_gamePk(file_content):
    data = json.loads(file_content)
    return data.get('scoreboard', {}).get('gamePk', 'N/A')



def debug_scoreboard_column(df):
   
    for i, row in df.iterrows():
        scoreboard_data = row['scoreboard']
        print("Type:", type(scoreboard_data))
        print("Content:", scoreboard_data)



def extract_team_id(file_content):
    try:
        data = json.loads(file_content)
        team_id = data['boxscore']['teams']['away']['team']['id']
        return team_id
    except (KeyError, TypeError):
        return 'N/A'


def extract_team_name(file_content):
    try:
        data = json.loads(file_content)
        team_name = data['boxscore']['teams']['away']['team']['name']
        return team_name
    except (KeyError, TypeError):
        return 'N/A'


def process_json_file(file_path,gamePk,team_id,team_name):
    # Reading JSON file using Dask



    ddf = dd.read_json(file_path, orient='records', lines=True)

    ddf['gamePk'] = gamePk
    ddf['team_id'] = team_id
    ddf['team_name'] = team_name


    ddf = ddf.drop(['scoreboard', 'boxscore', 'home_team_data', 'away_team_data'], axis=1)  
    ddf = ddf.fillna('N/A')   
    list_columns = ['away_lineup', 'home_lineup', 'away_pitcher_lineup', 'home_pitcher_lineup']
    for col in list_columns:
        ddf[col] = ddf[col].apply(lambda x: x if isinstance(x, list) else [])     
        meta = {c: 'object' for c in ddf.columns}
    print(ddf.head())
    return ddf


def main():
    client = Client()
    s3_client = boto3.client('s3')
    source_bucket = 'mlb-data-store'
    target_bucket = 'mlb-data-clean'

    start_year = 2021
    start_month = 2

    for year in range(start_year, 2024):
    
        for month in range(start_month if year == start_year else 1, 13):  
            for day in range(1, 32): 
                
                formatted_month = f'{month:02d}'
                formatted_day = f'{day:02d}'

                file_path_pattern = f'games/{year}/{formatted_month}/{formatted_day}/'
                file_list = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=file_path_pattern)

                for file in file_list.get('Contents', []):
                    file_name = file['Key']
                    file_path = f's3://{source_bucket}/{file_name}'

                    logging.info(f"Processing file: {file_name}")

                    try:
                        obj = s3_client.get_object(Bucket=source_bucket, Key=file_name)
                        json_content = obj['Body'].read().decode('utf-8')
                        gamePk = extract_gamePk(json_content)
                        team_id = extract_team_id(json_content)
                        team_name = extract_team_name(json_content)
                        ddf = process_json_file(file_path, gamePk, team_id, team_name)                       
                        output_file_name = file_name.split('/')[-1].replace('.json', '.csv')
                        output_path = f's3://{target_bucket}/processed_games/{year}/{formatted_month}/{formatted_day}/{output_file_name}'
                        ddf.to_csv(output_path, single_file=True, index=False)
                        logging.info(f"Finished writing CSV for file: {file_name}")
                    except Exception as e:
                        logging.error(f"Error processing file {file_path}: {e}")

    logging.info("Data processing complete.")
    client.close()

if __name__ == "__main__":
    main()
