require('dotenv-safe').config();

const { Consumer } = require('sqs-consumer');
const AWS = require('aws-sdk');

AWS.config.update({
  region: 'ap-south-1',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new AWS.S3();

function getObjectSize(key, bucket) {
  return s3
    .headObject({ Key: key, Bucket: bucket })
    .promise()
    .then((res) => res.ContentLength);
}

const https = require('https');

const processS3Events = async (data) => {
  const { eventName, requestParameters, additionalEventData } = data;

  const regularCreateEvents = ['PutObject', 'CopyObject'];

  const multiPartCreateEvents = ['CreateMultipartUpload', 'UploadPart', 'CompleteMultipartUpload'];

  const targetEvents = [...regularCreateEvents, ...multiPartCreateEvents];

  if (targetEvents.includes(eventName)) {
    const { bucketName, key } = requestParameters;
    const { bytesTransferredIn } = additionalEventData;

    if (multiPartCreateEvents.includes(eventName)) {
      switch (eventName) {
        case 'CreateMultipartUpload':
          console.log('UPLOAD STARTED', { bucketName, key });
          break;
        case 'UploadPart':
          console.log('PART UPLOADED', { bucketName, key, partSize: bytesTransferredIn });
          break;
        case 'CompleteMultipartUpload':
          const sizeInBytes = await getObjectSize(key, bucketName);
          console.log('UPLOAD COMPLETE', { bucketName, key, sizeInBytes });
          break;
        default:
          console.warn('WARNING: Unhandled S3 event', eventName);
      }
    } else {
      console.log('FILE UPLOADED', { bucketName, key, sizeInBytes: bytesTransferredIn });
    }
  } else {
    console.warn('WARNING: Unhandled S3 event', eventName);
  }
};

const app = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL,
  handleMessage: async (message) => {
    const messageData = JSON.parse(message.Body);
    const { eventSource } = messageData.detail;
    if (eventSource === 's3.amazonaws.com') {
      processS3Events(messageData.detail);
    }
  },
  sqs: new AWS.SQS({
    httpOptions: {
      agent: new https.Agent({
        keepAlive: true,
      }),
    },
  }),
});

app.on('error', (err) => {
  console.error(err.message);
});

app.on('processing_error', (err) => {
  console.error(err.message);
});

const initSQSWatcher = () => {
  app.start();
};

module.exports = {
  initSQSWatcher,
};
