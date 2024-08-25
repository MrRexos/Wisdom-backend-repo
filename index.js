const express = require('express');
const jwt = require('jsonwebtoken');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
require('dotenv').config();

const myPool = mysql.createPool({
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: process.env.DATABASE,
  host: process.env.HOST,
  connectionLimit: 10
});

const app = express();
const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.get('/', (req, res) => {
  res.send({
    message: 'Default route'
  });
});

app.post('/api/users', (req, res) => {
  const { email, password } = req.body;

  myPool.getConnection((err, poolConnection) => {
    if (err) {
      console.error('Error making signup connection:', err);
      res.status(500).send({ message: 'Error making signup connection' });
      return;
    }

    poolConnection.query(
      'INSERT INTO user (email, password, isActive) VALUES (?, ?, 1)',
      [email, password],
      (err, insertResults) => {
        poolConnection.release(); // Liberar la conexión en todos los casos

        if (err) {
          console.error('Error inserting data into db:', err);
          res.status(500).send({ message: 'Error inserting user' });
        } else {
          res.status(201).send({ message: 'User inserted!' });
        }
      }
    );
  });
});

app.listen(PORT, () => {
  console.log(`Listening on PORT: ${PORT}`);
});
