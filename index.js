require("dotenv").config();
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const csv = require("csv-parser");
const { Readable } = require("stream");

const PAGE_SIZE = 1000;

const s3Client = new S3Client({
  region: "ap-south-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

async function* getPage(stream) {
  const buf = [];
  for await (const chunk of stream) {
    buf.push(chunk);
    if (buf.length === PAGE_SIZE) {
      yield buf;
    }
  }
  // yield if anything is left
  if (buf.length) {
    yield buf;
  }
}

// we are creating a closure so that
// we don't have to expose the stream
const iterator = async () => {
  const command = new GetObjectCommand({
    Bucket: process.env.BUCKET_NAME,
    Key: process.env.FILE_NAME,
  });

  const response = await s3Client.send(command);
  const stream = Readable.from(response.Body).pipe(csv());
  return {
    next: async () => getPage(stream).next(),
  };
};

const main = async () => {
  const itr = await iterator();
  while (true) {
    const { value, done } = await itr.next();
    if (done) {
      break;
    }
    console.log(value);
  }
};

main().catch(console.error);
