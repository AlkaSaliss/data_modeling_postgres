import os
from pathlib import Path
import glob
import json
import psycopg2
import psycopg2.extras
import pandas as pd
from io import StringIO
from datetime import datetime
import sys
import multiprocessing as mp
from tqdm.auto import tqdm
from sql_queries import *


def _pretty_print(msg):
    """A simple wrapper around the standard python print function, to print message `msg` 
    in a cool format
    """
    print("\n" + "*"*50)
    print(msg.upper())
    print("*"*50 + "\n")


def _read_json(p):
    """A helper function to load a single json from path `p` and return it as a pandas dataframe
    """
    return pd.read_json(p, lines=True)


def read_json_parallel(list_files_path):
    """Helper function for loading multiple json files from list of path `list_files_path`
    in parallel and concatenate them in single pandas dataframe
    """
    with mp.Pool() as pool:
        list_dfs = pool.map(_read_json, tqdm(list_files_path))
    list_dfs = pd.concat(list_dfs, ignore_index=True)
    _pretty_print(f"{len(list_files_path)} json files loaded")
    return list_dfs


def process_song_files(cur, list_files_path):
    """Reads json files as pandas dataframe from list_files_path containing songs data and inserts the data into `songs` and `artists` tables
    using cursor `cur` from sparkifydb database
    """

    _pretty_print("loading song data files")
    df = read_json_parallel(list_files_path)

    # INSERT ARTIST RECORDS
    _pretty_print("Loading artists data into `artists` table")
    try:
        psycopg2.extras.execute_batch(cur, artist_table_insert,
                                      [item for item in df[["artist_id", "artist_name", "artist_location",
                                                            "artist_latitude", "artist_longitude"]].itertuples(index=False, name=None)],
                                      page_size=1000)
    except psycopg2.Error as e:
        print("Can't insert data into artists table")
        print(e)
        sys.exit()

    # INSERT SONG RECORDS
    _pretty_print("Loading song data into `songs` table")
    try:
        psycopg2.extras.execute_batch(cur, song_table_insert,
                                      [item for item in df[["song_id", "title", "artist_id", "year", "duration"]].itertuples(
                                          index=False, name=None)],
                                      page_size=1000)
    except psycopg2.Error as e:
        print("Can't insert data into songs table")
        print(e)
        sys.exit()


def process_log_files(cur, list_files_path):
    """Reads json files as pandas dataframe from list_files_path containing users activities data and inserts
     the data into ``time`, `users` and `songplays` tables using cursor `cur` from sparkifydb database
    """

    _pretty_print("reading logs data files")
    df = read_json_parallel(list_files_path)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df["new_ts"] = df.ts.apply(lambda x: datetime.fromtimestamp(x/1000))

    # insert time data records
    time_df = pd.DataFrame()
    time_df["start_time"] = df.new_ts
    time_df["hour"] = df.new_ts.dt.hour.astype(int)
    time_df["day"] = df.new_ts.dt.day.astype(int)
    time_df["week"] = df.new_ts.dt.isocalendar().week.astype(int)
    time_df["month"] = df.new_ts.dt.month.astype(int)
    time_df["year"] = df.new_ts.dt.year.astype(int)
    time_df["weekday"] = df.new_ts.dt.weekday.astype(int)

    _pretty_print("Loading timestamp data into `time` table")
    try:
        psycopg2.extras.execute_batch(cur, time_table_insert,
                                      [item for item in time_df[["start_time", "hour", "day", "week",
                                                                 "month", "year", "weekday"]].itertuples(index=False, name=None)],
                                      page_size=1000
                                      )
    except psycopg2.Error as e:
        print("Can't insert data into time table")
        print(e)
        sys.exit()

    # load user table
    # insert user records
    _pretty_print("Loading users data into `users` table")
    try:
        psycopg2.extras.execute_batch(cur, user_table_insert,
                                      [item for item in df[["userId", "firstName", "lastName", "gender", "level"]].itertuples(
                                          index=False, name=None)],
                                      page_size=1000
                                      )
    except psycopg2.Error as e:
        print("Can't insert data into users table")
        print(e)
        sys.exit()

    list_song_ids = []
    list_artist_ids = []
    for _, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        list_song_ids.append(songid)
        list_artist_ids.append(artistid)
    df["artist_ids"] = list_artist_ids
    df["song_ids"] = list_song_ids

    # insert songplay record
    _pretty_print("Loading song plays data into `songplays` table")
    try:
        psycopg2.extras.execute_batch(cur, songplay_table_insert,
                                      [item for item in
                                       df[["new_ts", "userId", "level", "song_ids", "artist_ids", "sessionId", "location", "userAgent"]].itertuples(index=False, name=None)],
                                      page_size=1000
                                      )
    except psycopg2.Error as e:
        print("Can't insert data into songplays table")
        print(e)
        sys.exit()


def process_data(cur, conn, filepath, func):
    """Loads all files from `filepath` and process them and insert the data into tables using `func` function.
    The connection `conn` and cursor `cur` are used for this aim.
    """

    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Apply func to list of files
    func(cur, all_files)
    conn.commit()


def main():
    """First load the json configuration file and use the credentials to connect to database;
    Next load the song data and log data into their respective tables.
    """
    with open(os.path.join(Path(__file__).parent.absolute(), "config.json")) as f:
        CONFIG = json.load(f)

    try:
        conn = psycopg2.connect(
            f"host={CONFIG['host']} dbname=sparkifydb user={CONFIG['user']} password={CONFIG['password']}")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Can't connect and get cursor from sparkifydb")
        print(e)
        sys.exit()

    song_data_path = os.path.join(
        Path(__file__).parent.absolute(), "data", "song_data")
    log_data_path = os.path.join(
        Path(__file__).parent.absolute(), "data", "log_data")
    process_data(cur, conn, filepath=song_data_path, func=process_song_files)
    process_data(cur, conn, filepath=log_data_path, func=process_log_files)

    conn.close()


if __name__ == "__main__":
    main()
