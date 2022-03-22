import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes the 'song_file' dataset in order to insert a record's data into the following tables:
        (dimensional) artists
        (dimensional) songs

    Args:
        (psycopg2.cursor) cur - database cursor
        (str) filepath - the filepath for the 'song_file'

    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id','title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the 'log_file' dataset in order to insert a record's data into the following tables:
        (dimensional) time
        (dimensional) users
        (fact) songplays     
    
    Args:
        (psycopg2.cursor) cur - database cursor
        (str) filepath - the filepath for the 'log_file'

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_filter = df['page'] == 'NextSong'
    df = df[df_filter]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.copy()
    
    # insert time data records
    time_data = (t.ts, t.ts.dt.hour , t.ts.dt.day , t.ts.dt.week , t.ts.dt.month , t.ts.dt.year , t.ts.dt.weekday)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df =  pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes JSON files from a directory path.
    
    Valid function values can be 'process_song_file' or
    'process_log_file'.
    
    Args:
        (psycopg2.cursor) cur - database cursor
        (psycopg2.connection) conn - database connection
        (str) filepath - the filepath for the directory to be processed
        (function) func - the function to call for each found file
        
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main method, acts as the point of execution for the program.
    
    Creates a database connection and cursor.
    Processes the 'song_file' and 'log_file'.
    Closes the cursor and database connection.
    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()