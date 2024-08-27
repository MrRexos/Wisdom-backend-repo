const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware para parsear JSON
app.use(bodyParser.json());
app.use(express.json());

// Configuración del pool de conexiones a la base de datos
const pool = mysql.createPool({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: process.env.DATABASE,
  waitForConnections: true,
  connectionLimit: 10,  // Número máximo de conexiones en el pool
  queueLimit: 0
});

// Ruta de prueba
app.get('/', (req, res) => {
  res.send('El backend está funcionando.');
});

// Ruta para obtener usuarios
app.get('/api/users', (req, res) => {
    pool.query('SELECT * FROM user_account', (err, results) => {
      if (err) {
        console.error('Error al obtener usuarios:', err);
        res.status(500).json({ error: 'Error al obtener usuarios.' });
        return;
      }
      res.json(results);
    });
  });

// Ruta para crear un nuevo usuario
app.post('/api/signup', (req, res) => {
  const { email, username, password, first_name, surname, language, allow_notis } = req.body;
  const query = 'INSERT INTO user_account (email, username, password, first_name, surname, joined_datetime, language, allow_notis) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?)';
  const values = [ email, username, password, first_name, surname, language, allow_notis];
  pool.query(query, values, (err, results) => {
    if (err) {
      console.error('Error al crear el usuario:', err);
      res.status(500).send('Error al crear el usuario.');
      return;
    }
    res.status(201).send('Usuario creado.');
  });
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});
