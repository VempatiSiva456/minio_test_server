const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const Minio = require("minio");
const fs = require("fs");
const path = require("path");
const stream = require("stream");

const app = express();

const minioClient = new Minio.Client({
  endPoint: "localhost", // Change to your MinIO server endpoint
  port: 9000, // Change to your MinIO server port
  useSSL: false,
  accessKey: "minioadmin", // Change to your access key
  secretKey: "minioadmin", // Change to your secret key
});

let ongoingUploads = new Map();
const bucketName = "uploads";

createBucketIfNotExists(bucketName);

async function createBucketIfNotExists(bucketName) {
  try {
    const exists = await minioClient.bucketExists(bucketName);
    if (!exists) {
      await minioClient.makeBucket(bucketName);
    }
  } catch (error) {
    throw error;
  }
}

app.use(cors());
app.use(bodyParser.raw({ limit: "10mb", type: "application/octet-stream" }));

// Function to delete a folder and its contents in MinIO
async function cleanupFolder(folderPath) {
  const objectsStream = minioClient.listObjects(bucketName, folderPath, true);
  for await (const obj of objectsStream) {
    await minioClient.removeObject(bucketName, obj.name);
  }
}

// Endpoint to delete partial uploads and their chunks
app.post("/api/upload/cancel", async (req, res) => {
  const fileName = Buffer.from(req.headers["x-file-name"], "base64").toString();
  if (!fileName) {
    return res.status(400).send("File name required");
  }

  const tempFolderPath = `temp/${fileName}/`;
  const uploadStatus = ongoingUploads.get(fileName) || {
    pendingChunks: new Set(),
  };
  uploadStatus.canceled = true;
  ongoingUploads.set(fileName, uploadStatus);

  // Wait for ongoing uploads to complete before cleanup
  const checkInterval = setInterval(async () => {
    if (uploadStatus.pendingChunks.size === 0) {
      clearInterval(checkInterval);
      ongoingUploads.delete(fileName); // Remove the upload status

      try {
        // Delete the entire folder containing the chunks
        await cleanupFolder(tempFolderPath);
        console.log(`Canceled upload and deleted chunks for ${fileName}`);
        res.status(200).send("Upload canceled and chunks deleted successfully");
      } catch (error) {
        console.error(
          `Error canceling upload and deleting chunks for ${fileName}:`,
          error
        );
        res.status(500).send("Error canceling upload and deleting chunks");
      }
    }
  }, 1000); // Check every second
});

// Endpoint to delete all files
let isDeletingAll = false;

app.delete("/api/upload/deleteAll", async (req, res) => {
  if (isDeletingAll) {
    return res.status(400).send("Deletion already in progress");
  }

  try {
    isDeletingAll = true;

    const bucketExists = await minioClient.bucketExists(bucketName);
    if (!bucketExists) {
      return res.status(404).send("Bucket not found");
    }

    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      await minioClient.removeObject(bucketName, obj.name);
    }

    await minioClient.removeBucket(bucketName);
    console.log("All files deleted successfully");
    res.status(200).send("All files deleted successfully");
  } catch (error) {
    console.error("Error deleting all files:", error);
    res.status(500).send("Error deleting all files");
  } finally {
    isDeletingAll = false;
  }
});

app.get("/api/uploadfiles", async (req, res) => {
  const bucketExists = await minioClient.bucketExists(bucketName);
  if (!bucketExists) {
    return res.status(200).send("No Files to Download :(");
  }

  try {
    let fileSet = new Set();
    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      // Include only files outside the 'temp/' folder
      if (!obj.name.startsWith("temp/")) {
        fileSet.add(obj.name);
      }
    }
    const fileListHtml = Array.from(fileSet)
      .map(
        (fileName) =>
          `<div class="file-item">${fileName}<button class="download-button" data-filename="${fileName}">Download</button></div>`
      )
      .join("");
    res.send(fileListHtml);
  } catch (error) {
    console.error("Error listing files:", error);
    res.status(500).send("Error listing files");
  }
});

// Endpoint to list files
app.get("/api/upload", async (req, res) => {
  const bucketExists = await minioClient.bucketExists(bucketName);
  if (!bucketExists) {
    return res.status(200).send("No Files Uploaded Yet!");
  }

  try {
    let fileSet = new Set();
    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      // Include only files outside the 'temp/' folder
      if (!obj.name.startsWith("temp/")) {
        fileSet.add(obj.name);
      }
    }
    const fileListHtml = Array.from(fileSet)
      .map(
        (fileName) =>
          `<div class="file-item">${fileName}<button class="delete-button" data-filename="${fileName}">Delete</button></div>`
      )
      .join("");
    res.send(fileListHtml);
  } catch (error) {
    console.error("Error listing files:", error);
    res.status(500).send("Error listing files");
  }
});

// Endpoint to download a specific chunk of a file
app.get("/api/upload/download/:fileName/:chunkIndex", async (req, res) => {
  const fileName = req.params.fileName;
  const chunkIndex = req.params.chunkIndex;
  const chunkName = `${fileName}.${chunkIndex}`;
  const tempFolderPath = `temp/${fileName}/`;

  try {
    const dataStream = await minioClient.getObject(
      bucketName,
      tempFolderPath + chunkName
    );
    dataStream.pipe(res); // Stream the chunk to the client
  } catch (error) {
    console.error("Error downloading chunk:", error);
    res.status(500).send("Error downloading chunk");
  }
});

// Endpoint to get file metadata including chunk count
app.get("/api/upload/metadata/:fileName", async (req, res) => {
  const fileName = req.params.fileName;
  const tempFolderPath = `temp/${fileName}/`;

  try {
    let chunkCount = 0;
    const objectsStream = minioClient.listObjects(
      bucketName,
      tempFolderPath,
      true
    );
    for await (const obj of objectsStream) {
      if (obj.name.startsWith(tempFolderPath)) {
        chunkCount++;
      }
    }
    res.json({ chunkCount });
  } catch (error) {
    console.error("Error retrieving file metadata:", error);
    res.status(500).send("Error retrieving file metadata");
  }
});

// Endpoint to delete a file and its associated chunks
app.delete("/api/upload/delete/:fileName", async (req, res) => {
  const fileName = req.params.fileName;
  const tempFolderPath = `temp/${fileName}/`;
  const assembledFilePath = `${fileName}`;

  try {
    // Delete the assembled file
    await minioClient.removeObject(bucketName, assembledFilePath);

    // Delete all chunks in the temporary folder
    const objectsStream = minioClient.listObjects(
      bucketName,
      tempFolderPath,
      true
    );
    for await (const obj of objectsStream) {
      await minioClient.removeObject(bucketName, obj.name);
    }

    res.status(200).send("File and associated chunks deleted successfully");
  } catch (error) {
    console.error("Error deleting file and chunks:", error);
    res.status(500).send("Error deleting file and chunks");
  }
});

// Endpoint to upload chunks of a file and assembled
app.post("/api/upload", async (req, res) => {
  const fileName = Buffer.from(req.headers["x-file-name"], "base64").toString();
  const chunkIndex = parseInt(req.headers["x-chunk-index"], 10);
  const totalChunks = parseInt(req.headers["x-total-chunks"], 10); // Add this if total chunks count is sent from client
  const chunkName = `${fileName}.${chunkIndex}`;
  const tempChunkPath = `temp/${fileName}/${chunkName}`;

  const uploadStatus = ongoingUploads.get(fileName) || {
    totalChunks: totalChunks, // Assuming total chunks count is known beforehand
    uploadedChunks: new Set(),
    pendingChunks: new Set(),
    canceled: false,
  };

  if (uploadStatus.canceled) {
    return res.status(400).send("Upload canceled");
  }

  uploadStatus.pendingChunks.add(chunkIndex);
  ongoingUploads.set(fileName, uploadStatus);

  try {
    await minioClient.putObject(bucketName, tempChunkPath, req.body);
    uploadStatus.uploadedChunks.add(chunkIndex);

    if (uploadStatus.uploadedChunks.size === uploadStatus.totalChunks) {
      await assembleFile(fileName)
        .then(() => {
          res.status(200).send({
            message: "Chunk uploaded successfully",
            fileAssembled: true,
          });
          ongoingUploads.delete(fileName); // Clear upload status
        })
        .catch((error) => {
          console.error("Error in file assembly:", error);
          res.status(500).send("Error in file assembly");
        });
    } else {
      uploadStatus.pendingChunks.delete(chunkIndex);
      res
        .status(200)
        .send({ message: "Chunk uploaded successfully", fileAssembled: false });
    }
  } catch (error) {
    uploadStatus.pendingChunks.delete(chunkIndex);
    console.error("Error uploading chunk:", error);
    res.status(500).send("Error uploading chunk");
  }
});

async function assembleFile(fileName) {
  const tempFolderPath = `temp/${fileName}/`;
  const assembledFilePath = `${fileName}`;

  try {
    // List all chunks and sort them
    const objectsStream = minioClient.listObjects(
      bucketName,
      tempFolderPath,
      true
    );
    const chunkNames = [];
    for await (const obj of objectsStream) {
      if (obj.name.startsWith(tempFolderPath)) {
        chunkNames.push(obj.name);
      }
    }
    const sortedChunkNames = sortChunks(chunkNames);

    // Create a writable stream for the assembled file
    const assembledFileStream = new stream.PassThrough();

    // Initiate the putObject operation with the stream
    minioClient.putObject(bucketName, assembledFilePath, assembledFileStream);

    // Stream each chunk to the assembled file stream
    for (const chunkName of sortedChunkNames) {
      const chunkStream = await minioClient.getObject(bucketName, chunkName);
      await streamToPassThrough(chunkStream, assembledFileStream);
    }

    // Finalize the assembled file stream
    assembledFileStream.end();
    console.log(`${fileName} assembled and stored successfully`);

    // Return a promise that resolves when the file is fully assembled
    return new Promise((resolve, reject) => {
      assembledFileStream.on("finish", () => resolve(true));
      assembledFileStream.on("error", (error) => reject(error));
    });
  } catch (error) {
    console.error("Error assembling file:", error);
    throw error;
  }
}

function streamToPassThrough(src, dest) {
  return new Promise((resolve, reject) => {
    src.on("error", reject);
    src.on("end", resolve);
    src.pipe(dest, { end: false });
  });
}

function sortChunks(chunks) {
  return chunks.sort((a, b) => {
    const indexA = parseInt(a.substring(a.lastIndexOf(".") + 1));
    const indexB = parseInt(b.substring(b.lastIndexOf(".") + 1));
    return indexA - indexB;
  });
}

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
