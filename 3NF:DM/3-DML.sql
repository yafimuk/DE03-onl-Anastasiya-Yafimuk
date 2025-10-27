-- Inserting into albums table (surrogate key, no conflict handling needed as album_id is numeric)
INSERT INTO spotify.albums (album_name, release_year)
SELECT DISTINCT album, "year"
FROM spotify.spotifytophits_source a
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.albums WHERE album_name = a.album
);

-- Inserting into artists table (surrogate key with business key)
-- Avoid duplicates based on artist_name
INSERT INTO spotify.artists (artist_name, artist_popularity)
SELECT DISTINCT artist_name, artist_popularity
FROM spotify.spotifytophits_source at
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.artists WHERE artist_name = at.artist_name
);

WITH unnested_genres AS (
    SELECT
        s.artist_name,
        TRIM(LOWER(genre)) AS genre -- Unnest and make genre names case-insensitive
    FROM spotify.spotifytophits_source s,
         unnest(string_to_array(
             REPLACE(REPLACE(LOWER(s.artist_genres), '[', ''), ']', ''), -- Remove '[' and ']'
             ',')) AS genre
)

--Insert genres into genres table (only unique genres)
INSERT INTO spotify.genres (genre_name)
SELECT DISTINCT genre
FROM unnested_genres u
WHERE NOT EXISTS (
    SELECT 1 
    FROM spotify.genres g 
    WHERE TRIM(u.genre) = TRIM(LOWER(g.genre_name))
);

-- Inserting into artist_genres table (many-to-many relationship between artists and genres)
-- Ensure that both artist_id and genre_id are already inserted before
WITH unnested_genres AS (
    SELECT
        s.artist_name,
        TRIM(LOWER(genre)) AS genre -- Unnest and make genre names case-insensitive
    FROM spotify.spotifytophits_source s,
         unnest(string_to_array(
             REPLACE(REPLACE(LOWER(s.artist_genres), '[', ''), ']', ''), -- Remove '[' and ']'
             ',')) AS genre
)

INSERT INTO spotify.artist_genres (artist_id, genre_id)
SELECT DISTINCT a.artist_id, g.genre_id
FROM unnested_genres u
JOIN spotify.artists a ON TRIM(LOWER(u.artist_name)) = TRIM(LOWER(a.artist_name))
JOIN spotify.genres g ON TRIM(u.genre) = TRIM(LOWER(g.genre_name))
WHERE NOT EXISTS (
    SELECT 1
    FROM spotify.artist_genres ag_exist
    WHERE ag_exist.artist_id = a.artist_id AND ag_exist.genre_id = g.genre_id
);


-- Inserting into tracks table (surrogate key with business key)
-- Ensure that albums are already inserted and use track_business_key to avoid duplicates
INSERT INTO spotify.tracks (track_name, track_popularity, track_business_key)
SELECT DISTINCT t.track_name, t.track_popularity, t.track_id
FROM spotify.spotifytophits_source t
JOIN spotify.albums a ON t.album = a.album_name
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.tracks WHERE track_business_key = t.track_id
);

-- Inserting into playlists table (surrogate key with business key)
-- Avoid duplicates based on playlist_url
INSERT INTO spotify.playlists (playlist_url, playlist_name, playlist_business_key)
SELECT DISTINCT playlist_url, 'Top Hits Playlist', playlist_url
FROM spotify.spotifytophits_source pt
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.playlists WHERE playlist_url = pt.playlist_url
);

-- Inserting into playlist_tracks table (many-to-many relationship between playlists and tracks)
-- Ensure that both playlist_id and track_id are already inserted before
INSERT INTO spotify.playlist_tracks (playlist_id, track_id)
SELECT DISTINCT p.playlist_id, t.track_id
FROM spotify.spotifytophits_source pt
JOIN spotify.playlists p ON pt.playlist_url = p.playlist_url
JOIN spotify.tracks t ON pt.track_id = t.track_business_key
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.playlist_tracks WHERE playlist_id = p.playlist_id AND track_id = t.track_id
);

-- Inserting into artist_tracks table (many-to-many relationship between artists and tracks)
-- Ensure that both artist_id and track_id are already inserted before
INSERT INTO spotify.artist_tracks (artist_id, track_id)
SELECT DISTINCT a.artist_id, t.track_id
FROM spotify.spotifytophits_source at
JOIN spotify.artists a ON at.artist_name = a.artist_name
JOIN spotify.tracks t ON at.track_id = t.track_business_key
WHERE NOT EXISTS (
    SELECT 1
    FROM spotify.artist_tracks
    WHERE artist_id = a.artist_id AND track_id = t.track_id
);

-- Inserting into audio_features table (surrogate key with business key)
-- Ensure that tracks are already inserted and use track_id to avoid duplicates
INSERT INTO spotify.audio_features (danceability, energy, loudness, "key", track_id, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature)
SELECT DISTINCT af.danceability, af.energy, af.loudness, af."key", t.track_id, af."mode", af.speechiness, af.acousticness, af.instrumentalness, af.liveness, af.valence, af.tempo, af.duration_ms, af.time_signature
FROM spotify.spotifytophits_source af
JOIN spotify.tracks t ON af.track_id = t.track_business_key
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.audio_features WHERE track_id = t.track_id
);


-- Inserting into track_album bridge table (many-to-many relationship between tracks and albums)
-- Ensure that tracks and albums are already inserted before
INSERT INTO spotify.track_albums (track_id, album_id)
SELECT DISTINCT t.track_id, a.album_id
FROM spotify.spotifytophits_source ta
JOIN spotify.tracks t ON ta.track_id = t.track_business_key
JOIN spotify.albums a ON ta.album = a.album_name
WHERE NOT EXISTS (
    SELECT 1 FROM spotify.track_albums WHERE track_id = t.track_id AND album_id = a.album_id
);
