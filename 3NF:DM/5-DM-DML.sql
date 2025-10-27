-- Insert artists into dim_artists (ensure unique)
INSERT INTO dm_spotify.dim_artists (artist_id, artist_name, artist_popularity)
SELECT DISTINCT artist_id, artist_name, artist_popularity
FROM spotify.artists
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.dim_artists da
    WHERE da.artist_name = spotify.artists.artist_name
);


-- Insert genres into dim_genres (ensure unique)
INSERT INTO dm_spotify.dim_genres (genre_id, genre_name)
SELECT DISTINCT genre_id, genre_name
FROM spotify.genres
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.dim_genres dg
    WHERE dg.genre_name = spotify.genres.genre_name
);

-- Insert artist-genre relationships into artist_genres (ensure unique combination)
INSERT INTO dm_spotify.artist_genres (artist_id, genre_id)
SELECT DISTINCT ag.artist_id, ag.genre_id
FROM spotify.artist_genres ag
JOIN dm_spotify.dim_artists a ON ag.artist_id = a.artist_id
JOIN dm_spotify.dim_genres g ON ag.genre_id = g.genre_id
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.artist_genres ag2
    WHERE ag2.artist_id = ag.artist_id AND ag2.genre_id = ag.genre_id
);

-- Insert albums into dim_albums (ensure unique)
INSERT INTO dm_spotify.dim_albums (album_id, album_name, release_year)
SELECT DISTINCT album_id, album_name, release_year
FROM spotify.albums
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.dim_albums da
    WHERE da.album_name = spotify.albums.album_name
);

-- Insert playlists into dim_playlists (ensure unique)
INSERT INTO dm_spotify.dim_playlists (playlist_id, playlist_url, playlist_name)
SELECT DISTINCT playlist_id, playlist_url, playlist_name
FROM spotify.playlists
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.dim_playlists dp
    WHERE dp.playlist_url = spotify.playlists.playlist_url
);

-- 4. Insert data into FACT table (fact_tracks)

INSERT INTO dm_spotify.fact_tracks (
    track_name, 
    track_popularity, 
    track_business_key, 
    artist_id, 
    album_id, 
    playlist_id, 
    danceability, 
    energy, 
    loudness, 
    "key", 
    "mode", 
    speechiness, 
    acousticness, 
    instrumentalness, 
    liveness, 
    valence, 
    tempo, 
    duration_ms, 
    time_signature
)
SELECT DISTINCT
    t.track_name,
    t.track_popularity,
    t.track_business_key,
    a.artist_id,
    al.album_id,
    p.playlist_id,
    af.danceability,
    af.energy,
    af.loudness,
    af."key",
    af."mode",
    af.speechiness,
    af.acousticness,
    af.instrumentalness,
    af.liveness,
    af.valence,
    af.tempo,
    af.duration_ms,
    af.time_signature
FROM spotify.tracks t
JOIN spotify.artist_tracks at ON t.track_id = at.track_id
JOIN spotify.artists a ON at.artist_id = a.artist_id
JOIN spotify.track_albums ta ON t.track_id = ta.track_id
JOIN spotify.albums al ON ta.album_id = al.album_id
JOIN spotify.playlist_tracks pt ON t.track_id = pt.track_id
JOIN spotify.playlists p ON pt.playlist_id = p.playlist_id
JOIN spotify.audio_features af ON t.track_id = af.track_id
WHERE NOT EXISTS (
    SELECT 1
    FROM dm_spotify.fact_tracks ft
    WHERE 
        ft.track_business_key = t.track_business_key
        AND ft.artist_id = a.artist_id
        AND ft.album_id = al.album_id
        AND ft.playlist_id = p.playlist_id
);


 