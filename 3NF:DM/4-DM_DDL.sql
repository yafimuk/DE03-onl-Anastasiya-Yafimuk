-- Create schema for the star schema (data mart)
CREATE SCHEMA IF NOT EXISTS dm_spotify;

-- Dimension table for artists (dim_artists)
CREATE TABLE IF NOT EXISTS dm_spotify.dim_artists (
    artist_id INT PRIMARY KEY, -- Surrogate key for artist
    artist_name VARCHAR(255) NOT NULL, -- Artist's name
    artist_popularity INT -- Popularity rating of the artist
);

-- Dimension table for albums (dim_albums)
CREATE TABLE IF NOT EXISTS dm_spotify.dim_albums (
    album_id INT PRIMARY KEY, -- Surrogate key for album
    album_name VARCHAR(255) NOT NULL, -- Album's name
    release_year INT -- Year the album was released
);

-- Dimension table for genres (dim_genres)
CREATE TABLE IF NOT EXISTS dm_spotify.dim_genres (
    genre_id INT PRIMARY KEY, -- Surrogate key for genre
    genre_name VARCHAR(255) NOT NULL UNIQUE -- Genre name (must be unique)
);

-- Dimension table for playlists (dim_playlists)
CREATE TABLE IF NOT EXISTS dm_spotify.dim_playlists (
    playlist_id INT PRIMARY KEY, -- Surrogate key for playlist
    playlist_url VARCHAR(255) UNIQUE NOT NULL, -- Playlist URL (unique identifier)
    playlist_name VARCHAR(255) NOT NULL -- Name of the playlist
);

-- Bridge table to represent the many-to-many relationship between artists and genres
CREATE TABLE IF NOT EXISTS dm_spotify.artist_genres (
    artist_id INT, -- Foreign key to dim_artists
    genre_id INT, -- Foreign key to dim_genres
    PRIMARY KEY (artist_id, genre_id), -- Composite primary key
    FOREIGN KEY (artist_id) REFERENCES dm_spotify.dim_artists(artist_id) ON DELETE CASCADE, -- Deletes in artist_genres if an artist is deleted
    FOREIGN KEY (genre_id) REFERENCES dm_spotify.dim_genres(genre_id) ON DELETE CASCADE -- Deletes in artist_genres if a genre is deleted
);

-- Fact table for tracks (fact_tracks) with audio features and references to dimension tables
CREATE TABLE IF NOT EXISTS dm_spotify.fact_tracks (
    fact_id SERIAL PRIMARY KEY,  -- Surrogate Key
    track_name VARCHAR(255),
    track_popularity INT,
    track_business_key VARCHAR(255),
    artist_id INT,
    album_id INT,
    playlist_id INT,
    genre_id INT,
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    "key" INT,
    "mode" INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INT,
    time_signature INT,
    FOREIGN KEY (artist_id) REFERENCES dm_spotify.dim_artists(artist_id),
    FOREIGN KEY (album_id) REFERENCES dm_spotify.dim_albums(album_id),
    FOREIGN KEY (playlist_id) REFERENCES dm_spotify.dim_playlists(playlist_id),
    -- Composite unique constraint on the combination of keys
    CONSTRAINT unique_track_combination UNIQUE (track_business_key, artist_id, album_id, playlist_id)
);