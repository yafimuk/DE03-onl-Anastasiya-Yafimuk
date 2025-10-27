-- Table for albums with surrogate key (no business key needed as album_id is numeric)
CREATE TABLE IF NOT EXISTS spotify.albums (
    album_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    album_name VARCHAR(255),
    release_year INT
);

-- Table for artists with surrogate key and business key (artist_id is VARCHAR in the original schema)
CREATE TABLE IF NOT EXISTS spotify.artists (
    artist_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    artist_name VARCHAR(255),
    artist_popularity INT
);

-- Table for genres with surrogate key (no business key needed as genre_id is numeric)
CREATE TABLE IF NOT EXISTS spotify.genres (
    genre_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    genre_name VARCHAR(255)
);

-- Table for tracks with surrogate key and business key (track_id is VARCHAR in the original schema)
CREATE TABLE IF NOT EXISTS spotify.tracks (
    track_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    track_name VARCHAR(255),
    track_popularity INT,
    track_business_key VARCHAR(255) UNIQUE -- Business key (original VARCHAR identifier)
);

-- Table for playlists with surrogate key and business key (playlist_url is VARCHAR in the original schema)
CREATE TABLE IF NOT EXISTS spotify.playlists (
    playlist_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    playlist_url VARCHAR(255) UNIQUE,
    playlist_name VARCHAR(255),
    playlist_business_key VARCHAR(255) UNIQUE -- Business key (original VARCHAR identifier)
);

-- Bridge table for many-to-many relationship between tracks and albums
CREATE TABLE IF NOT EXISTS spotify.track_albums (
    track_id INT,
    album_id INT,
    PRIMARY KEY (track_id, album_id),
    FOREIGN KEY (track_id) REFERENCES spotify.tracks(track_id),
    FOREIGN KEY (album_id) REFERENCES spotify.albums(album_id)
);

-- Table for playlist tracks with surrogate keys (no business keys needed as IDs are numeric)
CREATE TABLE IF NOT EXISTS spotify.playlist_tracks (
    playlist_id INT,
    track_id INT,
    PRIMARY KEY (playlist_id, track_id),
    FOREIGN KEY (playlist_id) REFERENCES spotify.playlists(playlist_id),
    FOREIGN KEY (track_id) REFERENCES spotify.tracks(track_id)
);

-- Table for artist tracks with surrogate keys (no business keys needed as IDs are numeric)
CREATE TABLE IF NOT EXISTS spotify.artist_tracks (
    artist_id INT,
    track_id INT,
    PRIMARY KEY (artist_id, track_id),
    FOREIGN KEY (artist_id) REFERENCES spotify.artists(artist_id),
    FOREIGN KEY (track_id) REFERENCES spotify.tracks(track_id)
);

-- Table for artist genres with surrogate keys (no business keys needed as IDs are numeric)
CREATE TABLE IF NOT EXISTS spotify.artist_genres (
    artist_id INT,
    genre_id INT,
    PRIMARY KEY (artist_id, genre_id),
    FOREIGN KEY (artist_id) REFERENCES spotify.artists(artist_id),
    FOREIGN KEY (genre_id) REFERENCES spotify.genres(genre_id)
);

-- Table for audio features with surrogate key and business key (track_id is VARCHAR in the original schema)
CREATE TABLE IF NOT EXISTS spotify.audio_features (
    feature_id SERIAL PRIMARY KEY, -- Surrogate key (automatically generated)
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    "key" INT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INT,
    time_signature INT,
    track_id INT,
    FOREIGN KEY (track_id) REFERENCES spotify.tracks(track_id)
);


