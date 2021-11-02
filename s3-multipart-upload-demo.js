require('dotenv-safe').config();
const fs = require('fs');
const AWS = require('aws-sdk');

let bucket = process.env.AWS_BUCKET;
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

// File
let fileName = 'fifa21.mp4';
let filePath = './' + fileName;
let fileKey = fileName;
let buffer = fs.readFileSync('./' + filePath);
// S3 Upload options

// Upload
let startTime = new Date();
let partNum = 0;
let partSize = 1024 * 1024 * 5; // Minimum 5MB per chunk (except the last part) http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
let numPartsLeft = Math.ceil(buffer.length / partSize);
let maxUploadTries = 3;
let multiPartParams = {
  Bucket: bucket,
  Key: fileKey,
  ContentType: 'application/pdf',
};
let multipartMap = {
  Parts: [],
};

console.log('=====Parts left===', numPartsLeft);

function completeMultipartUpload(s3, doneParams) {
  s3.completeMultipartUpload(doneParams, function (err, data) {
    if (err) {
      console.log('An error occurred while completing the multipart upload');
      console.log(err);
    } else {
      let delta = (new Date() - startTime) / 1000;
      console.log('Completed upload in', delta, 'seconds');
      console.log('Final upload data:', data);
    }
  });
}

function uploadPart(s3, multipart, partParams, tryNum) {
  tryNum = tryNum || 1;
  s3.uploadPart(partParams, function (multiErr, mData) {
    if (multiErr) {
      console.log('multiErr, upload part error:', multiErr);
      if (tryNum < maxUploadTries) {
        console.log('Retrying upload of part: #', partParams.PartNumber);
        uploadPart(s3, multipart, partParams, tryNum + 1);
      } else {
        console.log('Failed uploading part: #', partParams.PartNumber);
      }
      return;
    }
    multipartMap.Parts[this.request.params.PartNumber - 1] = {
      ETag: mData.ETag,
      PartNumber: Number(this.request.params.PartNumber),
    };
    console.log('Completed part', this.request.params.PartNumber);
    console.log('mData', mData);
    numPartsLeft = numPartsLeft - 1;

    console.log('=====Parts left===', numPartsLeft);

    if (numPartsLeft === 0) {
      // complete only when all parts uploaded
      let doneParams = {
        Bucket: bucket,
        Key: fileKey,
        MultipartUpload: multipartMap,
        UploadId: multipart.UploadId,
      };

      console.log('Completing upload...');
      completeMultipartUpload(s3, doneParams);
    }
  });
}

// Multipart
console.log('Creating multipart upload for:', fileKey);
s3.createMultipartUpload(multiPartParams, function (mpErr, multipart) {
  if (mpErr) {
    console.log('Error!', mpErr);
    return;
  }
  console.log('Got upload ID', multipart.UploadId);

  // Grab each partSize chunk and upload it as a part
  for (let rangeStart = 0; rangeStart < buffer.length; rangeStart += partSize) {
    partNum++;
    let end = Math.min(rangeStart + partSize, buffer.length),
      partParams = {
        Body: buffer.slice(rangeStart, end),
        Bucket: bucket,
        Key: fileKey,
        PartNumber: String(partNum),
        UploadId: multipart.UploadId,
      };

    // Send a single part
    console.log('Uploading part: #', partParams.PartNumber, ', Range start:', rangeStart);
    uploadPart(s3, multipart, partParams);
  }
});
