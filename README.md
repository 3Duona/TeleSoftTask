# TeleSoftTask
 Spotify data transformation and analysis task

# How to run solution

1. Download Spotify dataset from Kaggle (Link: https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks)
2. run CreateDB.sql to create database, tables, and views (example command: psql -U postgres -a -f CreateDB.sql)
3. run DataTransformation.js to transform data and upload it to AWS S3 (example command: node DataTranformation.js)
4. run DataLoading.js to load data from AWS S3 into locally hosted PostgreSQL (example command: node DataLoading.js)

### PostgreSQL Database ER Diagram
 Tables:
  tracks - Track data
  artists - Artist data
  artists_tracks - intermediary table to store artist id and id of the song artist has created
```mermaid
classDiagram
class tracks {
    + id : VARCHAR(255) PRIMARY KEY
    + name : VARCHAR(1000) NOT NULL
    + popularity : NUMERIC
    + duration_ms : NUMERIC
    + explicit : BOOLEAN,
    + danceability : VARCHAR(20)
    + energy : NUMERIC
    + key : NUMERIC
    + loudness : NUMERIC
    + mode : NUMERIC
    + speechiness : NUMERIC
    + acousticness : NUMERIC
    + instrumentalness : NUMERIC
    + liveness : NUMERIC
    + valence : NUMERIC
    + tempo : NUMERIC
    + time_signature : NUMERIC
    + release_year : INT
    + release_month : INT
    + release_day : INT
}

class artists {
    + id : VARCHAR(255) PRIMARY KEY
    + followers : NUMERIC
    + name : VARCHAR(255) NOT NULL
    + popularity : NUMERIC
}

class artists_tracks {
  + id : SERIAL PRIMARY KEY
  + artist_id : VARCHAR(255)
  + track_id : VARCHAR(255)
}

```

# Technologies used

1. Back-End: NodeJs
2. Data storage: AWS S3
3. Database: PostgreSQL
