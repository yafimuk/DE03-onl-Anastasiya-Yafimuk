-- Create or replace a view that provides analytics on artists and their tracks
CREATE OR REPLACE VIEW spotify.vw_artist_track_analytics AS
SELECT 
    a.artist_name,                                      -- Artist's name
    a.artist_popularity,                                -- Artist's popularity score
    t.track_business_key AS track_id,                  -- Business key for the track
    t.track_name,                                       -- Track name
    t.track_popularity,                                 -- Track's popularity score
    al.album_name,                                      -- Name of the album the track belongs to
    al.release_year,                                    -- Year the album was released
    STRING_AGG(DISTINCT g.genre_name, ', ') AS genres, -- Aggregated genres for the artist
    af.danceability,                                    -- Danceability score of the track
    af.energy,                                          -- Energy level of the track
    af.loudness,                                        -- Loudness level of the track
    af.tempo,                                           -- Tempo (BPM) of the track
    af.valence,                                         -- Positivity or mood score of the track
    af.duration_ms / 1000 AS duration_sec               -- Track duration in seconds
FROM spotify.tracks t
JOIN spotify.artist_tracks at ON t.track_id = at.track_id
JOIN spotify.artists a ON at.artist_id = a.artist_id
JOIN spotify.track_albums ta ON t.track_id = ta.track_id
JOIN spotify.albums al ON ta.album_id = al.album_id
JOIN spotify.artist_genres ag ON a.artist_id = ag.artist_id
JOIN spotify.genres g ON ag.genre_id = g.genre_id
JOIN spotify.audio_features af ON t.track_id = af.track_id
GROUP BY 
    a.artist_name, 
    a.artist_popularity, 
    t.track_business_key, 
    t.track_name, 
    t.track_popularity, 
    al.album_name, 
    al.release_year, 
    af.danceability, 
    af.energy, 
    af.loudness, 
    af.tempo, 
    af.valence, 
    af.duration_ms;
   
   
 select *
 from spotify.vw_artist_track_analytics
