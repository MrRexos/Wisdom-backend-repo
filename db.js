// db.js
import mysql from 'mysql2';

const db = mysql.createPool({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: process.env.NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

export { db };
