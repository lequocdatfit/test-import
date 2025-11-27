import { generate } from "csv-generate";
import fs from 'fs';

const run = async () => {
  const args = process.argv.slice(2); 
  console.log('args', args)
  const fileOutput = fs.createWriteStream(`task_${args[0]}.csv`);

  generate({
    columns: ["int", "ascii", "ascii"],
    length: Number(args[0])
  }).pipe(fileOutput);
}

run();