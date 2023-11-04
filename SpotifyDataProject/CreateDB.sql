DROP DATABASE IF EXISTS postgres;

CREATE DATABASE postgres;

-- Create 3 tables: tracks, artists, artists_tracks
CREATE TABLE tracks (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(1000) NOT NULL,
    popularity NUMERIC,
    duration_ms NUMERIC,
    explicit BOOLEAN,
    danceability VARCHAR(20),
    energy NUMERIC,
    key NUMERIC,
    loudness NUMERIC,
    mode NUMERIC,
    speechiness NUMERIC,
    acousticness NUMERIC,
    instrumentalness NUMERIC,
    liveness NUMERIC,
    valence NUMERIC,
    tempo NUMERIC,
    time_signature NUMERIC,
    release_year INT,
    release_month INT,
    release_day INT
);

CREATE TABLE artists (
    id VARCHAR(255) PRIMARY KEY,
    followers NUMERIC,
    name VARCHAR(255) NOT NULL,
    popularity NUMERIC
);

CREATE TABLE artists_tracks (
  id SERIAL PRIMARY KEY,
  artist_id VARCHAR(255),
  track_id VARCHAR(255)
);

-- Create view to take track: id, name, popularity, energy, danceability and artists followers
CREATE OR REPLACE VIEW track_artist_followers AS
SELECT
    t.id AS track_id,
    t.name AS track_name,
    t.popularity AS track_popularity,
    t.energy AS track_energy,
    t.danceability AS danceability_category,
    a.followers AS artist_followers
FROM tracks t
JOIN artists_tracks at ON t.id = at.track_id
JOIN artists a ON at.artist_id = a.id;

-- Create view to take only tracks of which artists has followers
CREATE OR REPLACE VIEW tracks_with_followers AS
SELECT
    t.name AS track_name,
    a.followers AS artist_followers
FROM tracks t
JOIN artists_tracks at ON t.id = at.track_id
JOIN artists a ON at.artist_id = a.id
WHERE a.followers IS NOT NULL AND a.followers > 0;

-- Create view to pick the most energizing track of each year
CREATE OR REPLACE VIEW most_energizing_tracks AS
WITH RankedTracks AS (
    SELECT
        t.name AS track_name,
        t.release_year,
        t.energy AS track_energy,
        ROW_NUMBER() OVER(PARTITION BY t.release_year ORDER BY t.energy DESC) AS rank
    FROM tracks t
)
SELECT
    track_name,
    release_year,
    track_energy
FROM RankedTracks
WHERE rank = 1;