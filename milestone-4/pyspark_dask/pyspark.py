import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, when
from urllib.parse import urlparse
from pyspark.sql.types import StringType

import logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def list_directories(s3_client, bucket, prefix):
    directories = []
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    for content in response.get('CommonPrefixes', []):
        directories.append(content['Prefix'])
    return directories
def list_files(s3_client, bucket, prefix):
    files = []
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for content in response.get('Contents', []):
        if content['Key'].endswith('.json'):
            files.append(content['Key'])
    return files
def process_json_file(spark, file_path):
    # Read JSON file
    df = spark.read.json(file_path)
    # Process 'schedule' section
    schedule_df = None
    if 'schedule' in df.columns:
        df_exploded_schedule = df.select(explode("schedule.dates").alias("dates"))
        games_df_schedule = df_exploded_schedule.select(
            col("dates.date").alias("date"),
            explode("dates.games").alias("game")
        )
        schedule_df = games_df_schedule.select(
            col("date"),
            col("game.gamePk").alias("gamePk"),
            col("game.season").alias("season"),
            col("game.teams.home.team.name").alias("home_team_name"),
            col("game.teams.away.team.name").alias("away_team_name"),
            col("game.teams.home.score").alias("home_score"),
            col("game.teams.away.score").alias("away_score"),
            col("game.venue.name").alias("venue_name"),
            col("game.status.abstractGameState").alias("game_state"),
            col("game.linescore.currentInningOrdinal").alias("current_inning"),
            col("game.officialDate").alias("official_date"),
            col("game.gameType").alias("game_type"),
            col("game.doubleHeader").alias("double_header"),
            col("game.gamedayType").alias("gameday_type"),
            col("game.tiebreaker").alias("tiebreaker"),
            col("game.gameNumber").alias("game_number"),
            col("game.seasonDisplay").alias("season_display"),
            col("game.dayNight").alias("day_night"),
            col("game.scheduledInnings").alias("scheduled_innings"),
            col("game.teams.home.leagueRecord.wins").alias("home_team_wins"),
            col("game.teams.home.leagueRecord.losses").alias("home_team_losses"),
            col("game.teams.away.leagueRecord.wins").alias("away_team_wins"),
            col("game.teams.away.leagueRecord.losses").alias("away_team_losses"),
            col("game.venue.id").alias("venue_id"),
            col("game.teams.home.team.id").alias("home_team_id"),
            col("game.teams.away.team.id").alias("away_team_id"),
        )
    # Process 'wpa' section
    wpa_df = None
    if 'wpa' in df.columns and 'ex' not in df.columns:
        df_wpa_exploded = df.select(explode("wpa").alias("wpa_item"))
        wpa_games_df = df_wpa_exploded.select(
            col("wpa_item.gamePk").alias("gamePk"),
            explode("wpa_item.wpaValues").alias("wpaValues")
        )
        wpa_df = wpa_games_df.select(
            col("gamePk").cast(StringType()),
            col("wpaValues.homeTeamWinProbability").alias("home_team_win_probability"),
            col("wpaValues.awayTeamWinProbability").alias("away_team_win_probability"),
            col("wpaValues.homeTeamWinProbabilityAdded").alias("home_team_win_probability_added"),
            col("wpaValues.hwp").alias("hwp"),
            col("wpaValues.awp").alias("awp"),
            col("wpaValues.atBatIndex").alias("atBatIndex")
        )
    # Combine 'schedule' and 'wpa' dataframes
    final_df = schedule_df
    if wpa_df is not None:
        final_df = final_df.join(wpa_df, "gamePk", "outer") if final_df is not None else wpa_df
    #  'N/A'
    final_df = final_df.fillna('N/A') if final_df is not None else spark.createDataFrame([], schema=None)
    return final_df






def main():
    setup_logging()
    spark = SparkSession.builder.appName("JSON to CSV Conversion").getOrCreate()
    s3_client = boto3.client('s3')

    source_bucket = 'mlb-data-store'
    target_bucket = 'mlb-data-clean'
    prefix = 'schedules/'

    start_year = 2002  
    years = list_directories(s3_client, source_bucket, prefix)

    for year_path in years:
        year = int(year_path.rstrip('/').split('/')[-1])
        if year < start_year:
            continue  

        logging.info(f"Processing data for the year: {year}")
        months = list_directories(s3_client, source_bucket, year_path)

        for month in months:
            logging.info(f"Processing data for month: {month}")
            files = list_files(s3_client, source_bucket, month)

            for file in files:
                file_path = f's3a://{source_bucket}/{file}'
                try:
                    logging.info(f"Processing file: {file}")
                    df = process_json_file(spark, file_path)
                    output_path = f's3a://{target_bucket}/{file.replace("schedules", "processed_schedules").replace(".json", ".csv")}'
                    df.write.csv(output_path, mode="overwrite", header=True)
                    logging.info(f"Finished writing CSV for file: {file}")
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
                    continue 

    logging.info("All data processing complete.")
    spark.stop()

if __name__ == "__main__":
    main()


