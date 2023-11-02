const fs = require("fs");
const csv = require("csv-parser");

const artistsCsvPath = "Content/artists.csv";
const tracksCsvPath = "Content/tracks.csv";

const artistsData = [];
const tracksData = [];

const AWS = require("aws-sdk");

AWS.config.update({
  accessKeyId: "AKIA5QQK7CIOQWP5TMXT",
  secretAccessKey: "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", // UXtgXsWRO0Q1Tmf4l655vlncuYbl7GbgteoRmSuf
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

      // Filter tracks that meet specific criteria
      const filteredTracks = tracksData.filter((track) => {
        return track.name && track.duration_ms >= 60000;
      });

      // Extract unique artist IDs from the filtered tracks
      const artistIds = new Set(filteredTracks.map((track) => track.artist_id));

      // Load only artists that have tracks after filtering
      const filteredArtists = artistsData.filter((artist) =>
        artistIds.has(artist.id)
      );

      // Explode track release date into separate columns
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

      // Finalize data transformation
      const finalTransformedTracks = transformedTracks.map((track) => ({
        ...track,
        danceability: mapDanceability(track.danceability),
      }));

      console.log("Transformed data:", finalTransformedTracks);
      console.log("Filtered artists:", filteredArtists);

      // AWS S3 data upload
      const transformedData = JSON.stringify(finalTransformedTracks);

      const bucketName = "spotifytaskdatastorage";
      const fileName = "transformed-data.json";

      const params = {
        Bucket: bucketName,
        Key: fileName,
        Body: transformedData,
        ContentType: "application/json",
      };

      s3.upload(params, (err, data) => {
        if (err) {
          console.error("Error uploading to S3:", err);
        } else {
          console.log("File uploaded successfully:", data.Location);
        }
      });
    });
}
