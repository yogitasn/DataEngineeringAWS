import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"


# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist VARCHAR(30) NOT NULL,
                                auth VARCHAR(10),
                                firstName VARCHAR(30),
                                gender VARCHAR(10),
                                itemInSession INTEGER,
                                lastName VARCHAR(10),
                                length FLOAT,
                                level  VARCHAR(10),
                                location VARCHAR(20),
                                method VARCHAR(10),
                                page VARCHAR(10),
                                registration INTEGER,
                                sessionId INTEGER,
                                song VARCHAR(100),
                                status VARCHAR(10)
                                ts TIMESTAMP,
                                userAgent VARCHAR(100),
                                userId INTEGER
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                             song_id varchar PRIMARY KEY,
                             num_songs INTEGER NOT NULL,
                             artist_id  VARCHAR(30) NOT NULL,
                             artist_latitude FLOAT,
                             artist_longitude FLOAT,
                             artist_location VARCHAR(30),
                             artist_name  VARCHAR(30),
                             song_id  VARCHAR(30),
                             title VARCHAR(40),
                             duration FLOAT,
                             year INTEGER);
                             
                         

""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                             songplay_id INTEGER NOT NULL,
                             start_time INTEGER NOT NULL, 
                             user_id INTEGER NOT NULL, 
                             level VARCHAR(9) NOT NULL, 
                             song_id VARCHAR(30) NOT NULL, 
                             artist_id VARCHAR(30) NOT NULL, 
                             session_id INTEGER NOT NULL,
                             location VARCHAR(20) NOT NULL, 
                             user_agent VARCHAR(100) NOT NULL);
""")

user_table_create = ("""CREATE TABLE user_table_create (
                          user_id     INTEGER NOT NULL,
                          first_name  VARCHAR(22) NOT NULL,
                          last_name   VARCHAR(22) NOT NULL,
                          gender      VARCHAR(10) NOT NULL,
                          level       VARCHAR(9) NOT NULL
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                                song_id VARCHAR(30) NOT NULL,
                                title   VARCHAR(22) NOT NULL,
                                artist_id VARCHAR(30) NOT NULL, 
                                year INTEGER NOT NULL, 
                                duration FLOAT NOT NULL
""")

artist_table_create = ("""CREATE TABLE artist_table_create (
                          artist_id    VARCHAR(30) NOT NULL,
                          name         VARCHAR(22) NOT NULL,
                          location     VARCHAR(22) NOT NULL,
                          lattitude    FLOAT NOT NULL,
                          longitude    FLOAT NOT NULL
                        );

""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                                start_time bigint NOT NULL, 
                                hour INTEGER NOT NULL,
                                day INTEGER NOT NULL, 
                                week INTEGER NOT NULL, 
                                month INTEGER NOT NULL,
                                year INTEGER NOT NULL, 
                                weekday INTEGER NOT NULL
                           );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(songplay_id,start_time,user_id, level, song_id, artist_id,                                             session_id, location, user_agent) \
                                    SELECT
                                    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,
                                    se.userId,
                                    se.level,
                                    ss.song_id,
                                    ss.artist_id,
                                    se.session_id,
                                    se.location,
                                    se.user_agent
                                    FROM staging_events se, staging_songs ss
                                    WHERE se.page = 'NextSong'
                                    AND se.song_title = ss.title
                                    AND se.artist_name = ss.artist_name
                                    AND se.song_length = ss.duration

""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name, gender, level) \
                                    VALUES(%s, %s, %s,%s,%s) 
                                    ON CONFLICT (user_id) 
                                    DO UPDATE SET level = users.level;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) \
                                    VALUES(%s, %s, %s,%s,%s)
                                    ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) \
                                      VALUES(%s, %s, %s,%s,%s) 
                                      ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""INSERT INTO  time(start_time, hour, day, week, month, year, weekday) \
                                     VALUES(%s, %s, %s,%s,%s,%s,%s);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
