import { Storage } from '@google-cloud/storage';
import express from 'express';
import PG from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "stream/promises";

const app = express();
app.use(express.json());

const port = 8080;

console.log('port', port)

const run = async (jobId, filePath) => {
  const storage = new Storage();
  const bucket = storage.bucket('taskford-bucket-local');
  const blob = bucket.file(filePath).createReadStream();

  const client = new PG.Client({
    user: "postgres",
    host: "localhost",
    database: "postgres",
    port: 5532,
  });

  await client.connect();

  const copyStream = client.query(copyFrom(`COPY task (id, summary, description) from STDIN WITH (FORMAT CSV)`));

  try {     
    await pipeline(blob, copyStream);
  } catch (err) {
    console.error(err);
  } 
  finally {
    copyStream.end();
    client.end();
  }
}

app.get("/health", (req, res) => {
  res.send(200);
});

app.post('/', (req, res) => {
  const { jobId, filePath } = req.body;
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