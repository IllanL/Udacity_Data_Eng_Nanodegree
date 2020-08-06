import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')

LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                                artist VARCHAR (200),
                                auth VARCHAR (200),
                                first_name VARCHAR (200),
                                gender CHAR(1),
                                item_session BIGINT,
                                last_name VARCHAR (200),
                                length NUMERIC(12,4),
                                level VARCHAR (200),
                                location VARCHAR (200),
                                method VARCHAR (200),
                                page BIGINT,
                                registration NUMERIC(12,4),
                                session_id BIGINT,
                                song TEXT,
                                status SMALLINT,
                                ts BIGINT,
                                user_agent VARCHAR (200),
                                user_id BIGINT )""")

staging_songs_table_create =  ("""CREATE  TABLE staging_songs(
                                    num_songs BIGINT,
                                    artist_id VARCHAR (200),
                                    artist_latitude NUMERIC(12,4),
                                    artist_longitude NUMERIC(12,4),
                                    artist_location VARCHAR (200),
                                    artist_name VARCHAR (200),
                                    song_id VARCHAR (200),
                                    title TEXT,
                                    duration NUMERIC(12,4),
                                    year INTEGER)""")



songplay_table_create =  ("""CREATE TABLE songplay(
                            songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id BIGINT NOT NULL,
                            level VARCHAR (200),
                            song_id VARCHAR (200),
                            artist_id VARCHAR (200),
                            session_id BIGINT,
                            location VARCHAR (200),
                            user_agent VARCHAR (200))""")


user_table_create = ("""CREATE TABLE users(
                        user_id BIGINT PRIMARY KEY,
                        first_name VARCHAR (200) NOT NULL,
                        last_name VARCHAR (200) NOT NULL,
                        gender CHAR(1),
                        level VARCHAR (200))""")

song_table_create = ("""CREATE TABLE songs(
                        song_id VARCHAR (200) PRIMARY KEY,
                        title VARCHAR (200),
                        artist_id VARCHAR (200),
                        year BIGINT,
                        duration NUMERIC )""")

artist_table_create = ("""CREATE TABLE artist(
                          artist_id VARCHAR (200) PRIMARY KEY,
                          name VARCHAR (200),
                          location VARCHAR (200),
                          latitude NUMERIC(12,4),
                          longitude NUMERIC(12,4) )""")

time_table_create = ("""CREATE TABLE time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour BIGINT,
                        day BIGINT,
                        week BIGINT,
                        month BIGINT,
                        year BIGINT,
                        weekDay BIGINT)""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {}""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto'""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT timestamp 'epoch' + a.ts/1000 * interval '1 second' AS start_time, 
                                    a.user_id, a.level, 
                                    b.song_id, b.artist_id, 
                                    a.session_id, a.location, a.user_agent
                            FROM staging_events AS a
                            JOIN staging_songs AS b
                            ON a.artist = b.artist_name
                            AND a.song = b.title
                            WHERE a.page = 'NextSong'""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT  user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
                        SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                            DATE_PART('hour', start_time), 
                            DATE_PART('day', start_time),
                            DATE_PART('week', start_time), 
                            DATE_PART('month', start_time),
                            DATE_PART('year', start_time), 
                            DATE_PART('dayofweek', start_time)
                            FROM stagingevents
                            WHERE page = 'NextSong'""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
