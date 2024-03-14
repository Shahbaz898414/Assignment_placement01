const express = require('express');

const csvtojson = require('csvtojson');

const fs = require('fs');

const { Pool } = require('pg');


const csvfilepath = "input.csv";

const jsonOutputPath = "output.json";


// const cnt = 'postgressql://postgres:0786@localhost:5432/postgres'

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: '0786',
  port: 5432,
});

const app = express();
const port = process.env.PORT || 4000;

// Parse CSV to JSON and save to output.json
csvtojson().fromFile(csvfilepath).then(json => {
  fs.writeFileSync(jsonOutputPath, JSON.stringify(json), "utf-8", (err) => {
    if (err) console.log(err);
  });

  console.log(json)

  // app.get('/',(req,res)=>{
  //   res.json(json);
  // })



  // Upload JSON data to PostgreSQL
  uploadToPostgreSQL(json);


});


// Upload JSON data to PostgreSQL
async function uploadToPostgreSQL(jsonData) {
  const client = await pool.connect();
  console.log(client)
  try {
    await client.query('BEGIN');

    for (const data of jsonData) {
      const queryText = `
        INSERT INTO public.users("name", age, address, gender)
        VALUES ($1, $2, $3, $4)
      `;
      const values = [
        JSON.stringify(data.name), 
        parseInt(data.age),
        JSON.stringify(data.address),
        data.gender
      ];

      await client.query(queryText, values);
    }

    await client.query('COMMIT');
    console.log('Data uploaded to PostgreSQL successfully shahbaz.');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error uploading data to PostgreSQL:', error);
  } finally {
    client.release();
  }
}

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});




/*

Hi,

I hope this message finds you well! I came across the (IND) Software Engineer II ( https://walmart.wd5.myworkdayjobs.com/WalmartExternal/job/IN-TN-CHENNAI-Home-Office-RMZ-Millenia-Biz-Park/XMLNAME--IND--Software-Engineer-II_R-1786350 ) at Walmart. As an aspiring Software Engineer, I admire the company's innovative work. If possible, could you kindly refer me? I'd greatly appreciate it!

Best regards,
Shahbaz Khan


My Resume:-
https://drive.google.com/file/d/1GWEPhW0xSo_SEXyfPfixQkT76R6srowC/view?usp=sharing



CREATE TABLE IF NOT EXISTS public.users ( id SERIAL PRIMARY KEY, name JSONB NOT NULL, age INT NOT NULL, address JSONB, gender VARCHAR );



*/