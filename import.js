import { Storage } from '@google-cloud/storage';
import express from 'express';
import PG from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "stream/promises";
import { createClient } from "redis"

const app = express();
app.use(express.json());

const port = 8080;

const pgClient = new PG.Client({
  host: process.env.HOST,
  port: process.env.DB_PORT,
  database: process.env.DATABASE,
  password: process.env.PASSWORD,
  user: process.env.USER,
});

await pgClient.connect();

const redisClient = createClient({
  url: process.env.REDIS_URL
});

await redisClient.connect();


const run = async (jobId, filePath) => {
  const storage = new Storage();
  const bucket = storage.bucket('taskford-bucket-local');
  const blob = bucket.file(filePath).createReadStream();
  const copyStream = pgClient.query(copyFrom(`COPY temp_task (id, summary, description) from STDIN WITH (FORMAT CSV)`));

  try {     
    await pipeline(blob, copyStream);
    console.log("done");
    await redisClient.publish(jobId, "done")
  } catch (err) {
    console.error(err);
  } 
  finally {
    copyStream.end();
    pgClient.end();
  }
}

app.get("/health", (req, res) => {
  res.send(200);
});

app.post('/', (req, res) => {
  // message from pub/sub
  console.log('req.body', JSON.stringify(req.body))
  const data = JSON.parse(Buffer.from(req.body.message.data, "base64").toString())
  const { jobId, filePath } = data;
  console.log('jobId', jobId, "path", filePath);
  
  run(jobId, filePath);
  res.send(200);
})


app.listen(port, (err) => {
  if (err) {
    return console.error(err);
  }
  console.log(`Example app listening on port ${port}`)
});


//  019a7c5f-268c-7ee7-8456-ad95be27f7e7/019ac612-3795-71ce-b000-f891e29ada99