const fs = require("fs");
const csv = require("csv-parser");
const AWS = require("aws-sdk");
const fastcsv = require("fast-csv");
const { PassThrough } = require("stream");

const artistsCsvPath = "Content/artists.csv";
const tracksCsvPath = "Content/tracks.csv";

const artistsData = [];
const tracksData = [];

AWS.config.update({
  accessKeyId: "yourkey",
  secretAccessKey: "yoursecretkey",
  region: "eu-north-1",
});

const s3 = new AWS.S3();

// Read and load data from artists.csv
fs.createReadStream(artistsCsvPath)
  .pipe(csv())
  .on("data", (row) => {
    artistsData.push(row);
  })
  .on("end", () => {
    console.log("Loaded data from artists.csv");
    processTracksData();
  });

function processTracksData() {
  // Read and load data from tracks.csv
  fs.createReadStream(tracksCsvPath)
    .pipe(csv())
    .on("data", (row) => {
      tracksData.push(row);
    })
    .on("end", () => {
      console.log("Loaded data from tracks.csv");

      // Filter tracks that have name and are longer than a minute
      const filteredTracks = tracksData.filter((track) => {
        return track.name && track.duration_ms >= 60000;
      });

      // Explode track release date into separate columns: year, month, day
      const transformedTracks = filteredTracks.map((track) => {
        const releaseDate = track.release_date.split("-");
        return {
          ...track,
          release_year: releaseDate[0],
          release_month: releaseDate[1],
          release_day: releaseDate[2],
        };
      });

      // Transform track danceability into string values
      function mapDanceability(danceability) {
        if (danceability >= 0 && danceability < 0.5) {
          return "Low";
        } else if (danceability >= 0.5 && danceability <= 0.6) {
          return "Medium";
        } else if (danceability > 0.6 && danceability <= 1) {
          return "High";
        }
      }

      // Finalize data transformation for tracks
      const finalTransformedTracks = transformedTracks.map((track) => ({
        ...track,
        danceability: mapDanceability(track.danceability),
      }));

      // Extract artist IDs from tracks and add them to artistIdsWithTracks
      const artistIdsWithTracks = new Set();
      finalTransformedTracks.forEach((track) => {
        const idArtistsArray = JSON.parse(track.id_artists.replace(/'/g, '"'));
        idArtistsArray.forEach((artistId) => {
          artistIdsWithTracks.add(artistId);
        });
      });

      // Filter artists data based on artist IDs in tracks
      const transformedArtists = artistsData.filter((artist) =>
        artistIdsWithTracks.has(artist.id)
      );

      // Create transformed CSV data
      const tracksCsvData = fastcsv.format({ headers: true });
      finalTransformedTracks.forEach((track) => tracksCsvData.write(track));
      tracksCsvData.end();

      const artistsCsvData = fastcsv.format({ headers: true });
      transformedArtists.forEach((artist) => artistsCsvData.write(artist));
      artistsCsvData.end();

      const bucketName = "spotifytaskdatastorage";

      const tracksReadStream = new PassThrough();
      tracksReadStream.end(tracksCsvData.read());

      const artistsReadStream = new PassThrough();
      artistsReadStream.end(artistsCsvData.read());

      // Upload transformedTracks.csv to AWS S3
      const tracksParams = {
        Bucket: bucketName,
        Key: "transformed-tracks.csv",
        Body: tracksReadStream,
        ContentType: "text/csv",
      };

      s3.upload(tracksParams, (err, data) => {
        if (err) {
          console.error("Error uploading transformed-tracks.csv to S3:", err);
        } else {
          console.log(
            "transformed-tracks.csv uploaded successfully:",
            data.Location
          );
        }
      });

      // Upload transformedArtists.csv to AWS S3
      const artistsParams = {
        Bucket: bucketName,
        Key: "transformed-artists.csv",
        Body: artistsReadStream,
        ContentType: "text/csv",
      };

      s3.upload(artistsParams, (err, data) => {
        if (err) {
          console.error("Error uploading transformed-artists.csv to S3:", err);
        } else {
          console.log(
            "transformed-artists.csv uploaded successfully:",
            data.Location
          );
        }
      });
    });
}
