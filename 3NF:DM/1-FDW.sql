CREATE SERVER IF NOT EXISTS file_fdw_server
FOREIGN DATA WRAPPER file_fdw;

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SCHEMA IF NOT EXISTS spotify;

CREATE FOREIGN TABLE IF NOT EXISTS spotify.spotifytophits_temp  (
    playlist_url VARCHAR,
    year INTEGER,
    track_id VARCHAR,
    track_name VARCHAR,
    track_popularity INTEGER,
    album VARCHAR,
    artist_id VARCHAR,
    artist_name VARCHAR,
    artist_genres VARCHAR,
    artist_popularity INTEGER,
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INTEGER,
    time_signature INTEGER
)
SERVER file_fdw_server
OPTIONS (filename 'C:/Users/Anastasiya_Yafimuk/OneDrive - EPAM/ITPU/SpotifyTopHitPlaylist2010to2022.csv', format 'csv', header 'true', delimiter ',');


CREATE TABLE IF NOT EXISTS spotify.spotifytophits_source (
    playlist_url VARCHAR,
    year INTEGER,
    track_id VARCHAR,
    track_name VARCHAR,
    track_popularity INTEGER,
    album VARCHAR,
    artist_id VARCHAR,
    artist_name VARCHAR,
    artist_genres VARCHAR,
    artist_popularity INTEGER,
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INTEGER,
    time_signature INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_playlist_url_track_id ON spotify.spotifytophits_source (playlist_url, track_id);

INSERT INTO spotify.spotifytophits_source
(playlist_url, year, track_id, track_name, track_popularity, album, artist_id, artist_name, artist_genres, artist_popularity, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature)
SELECT playlist_url, year, track_id, track_name, track_popularity, album, artist_id, artist_name, artist_genres, artist_popularity, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature
FROM spotify.spotifytophits_temp
ON CONFLICT (playlist_url, track_id) DO NOTHING;

SELECT * 
FROM spotify.spotifytophits_source s 

