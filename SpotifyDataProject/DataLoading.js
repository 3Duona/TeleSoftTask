const AWS = require("aws-sdk");
const pgp = require("pg-promise")();
const fastcsv = require("fast-csv");

AWS.config.update({
  accessKeyId: "AKIA5QQK7CIOQWP5TMXT",
  secretAccessKey: "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",
  region: "eu-north-1",
});

const s3ParamsTracks = {
  Bucket: "spotifytaskdatastorage",
  Key: "transformed-tracks.csv",
};

const s3ParamsArtists = {
  Bucket: "spotifytaskdatastorage",
  Key: "transformed-artists.csv",
};

const s3 = new AWS.S3();
const db = pgp("postgres://postgres:titas123@localhost:5432/postgres");

// Retrieve CSV data files from AWS S3
const retrieveDataFromS3 = (s3Params) => {
  return new Promise((resolve, reject) => {
    s3.getObject(s3Params, (err, data) => {
      if (err) {
        reject(err);
      } else {
        const csvData = data.Body.toString("utf-8");
        const results = [];

        fastcsv
          .parseString(csvData, { headers: true })
          .on("data", (row) => results.push(row))
          .on("end", () => resolve(results))
          .on("error", (err) => reject(err));
      }
    });
  });
};

const trackInsertQuery = `
    INSERT INTO tracks (
      id, name, popularity, duration_ms, explicit,
      danceability, energy, key, loudness, mode, speechiness,
      acousticness, instrumentalness, liveness, valence, tempo,
      time_signature, release_year, release_month, release_day
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id;
  `;

const artistInsertQuery = `
    INSERT INTO artists (
      id, followers, name, popularity
    )
    VALUES (
      $1, $2, $3, $4
    )
    ON CONFLICT DO NOTHING
  `;

const artistTrackInsertQuery = `
    INSERT INTO artists_tracks (artist_id, track_id)
    VALUES ($1, $2)
    ON CONFLICT DO NOTHING
  `;

const clearDataQueries = [
  "DELETE FROM tracks",
  "DELETE FROM artists",
  "DELETE FROM artists_tracks",
];

// Clear old data in all tables before inserting data
async function clearData() {
  for (const query of clearDataQueries) {
    try {
      await db.none(query);
      console.log(`Data cleared from table: ${query.split(" ")[2]}`);
    } catch (error) {
      console.error(
        `Error clearing data from table: ${query.split(" ")[2]}`,
        error
      );
    }
  }
}

(async () => {
  try {
    await clearData();

    // Load data from S3 for artists
    const artistsData = await retrieveDataFromS3(s3ParamsArtists);

    // Insert artists data into the PostgreSQL artists table
    for (const artist of artistsData) {
      try {
        await db.none(artistInsertQuery, [
          artist.id,
          artist.followers,
          artist.name,
          artist.popularity,
        ]);
      } catch (error) {
        console.error("Error inserting artist data:", error);
      }
    }

    // Load data from S3 for tracks
    const tracksData = await retrieveDataFromS3(s3ParamsTracks);

    // Insert the data into PostgreSQL tracks and artists_tracks tables
    for (const track of tracksData) {
      try {
        // Set default values for release year, month, and day
        const releaseYear = track.release_year || null;
        const releaseMonth = track.release_month || null;
        const releaseDay = track.release_day || null;

        const trackId = await db.one(trackInsertQuery, [
          track.id,
          track.name,
          track.popularity,
          track.duration_ms,
          track.explicit,
          track.danceability,
          track.energy,
          track.key,
          track.loudness,
          track.mode,
          track.speechiness,
          track.acousticness,
          track.instrumentalness,
          track.liveness,
          track.valence,
          track.tempo,
          track.time_signature,
          releaseYear,
          releaseMonth,
          releaseDay,
        ]);

        // Process track's artists and insert into artists_tracks intermediary table
        const artistIds = JSON.parse(track.id_artists.replace(/'/g, '"'));
        for (const artistId of artistIds) {
          try {
            await db.none(artistTrackInsertQuery, [artistId, track.id]);
          } catch (error) {
            console.error("Error inserting artists-track relationship:", error);
          }
        }
      } catch (error) {
        console.error("Error inserting track data:", error);
      }
    }
  } catch (error) {
    console.error("Error retrieving data from S3:", error);
  }
})();
