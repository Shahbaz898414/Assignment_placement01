

const csvParser = require('csv-parser');
const fs = require('fs');
const { Pool } = require('pg');

// Parse CSV file
function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const records = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row) => {
        records.push(row);
      })
      .on('end', () => {
        resolve(records);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

// Convert CSV records to JSON format
function convertToJSON(records) {
  const jsonRecords = records.map((record) => {
    const { 'name.firstName': firstName, 'name.lastName': lastName, age, ...additionalInfo } = record;
    const address = {
      line1: record['address.line1'],
      line2: record['address.line2'],
      city: record['address.city'],
      state: record['address.state'],
    };

    return {
      name: { firstName, lastName },
      age: parseInt(age),
      address,
      additionalInfo,
    };
  });

  return jsonRecords;
}

// Upload JSON records to PostgreSQL
async function uploadToPostgreSQL(jsonRecords) {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const record of jsonRecords) {
      const queryText = `
        INSERT INTO public.users ("name", age, address, additional_info)
        VALUES ($1, $2, $3, $4)
      `;
      const values = [`${record.name.firstName} ${record.name.lastName}`, record.age, JSON.stringify(record.address), JSON.stringify(record.additionalInfo)];

      await client.query(queryText, values);
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { parseCSV, convertToJSON, uploadToPostgreSQL };
