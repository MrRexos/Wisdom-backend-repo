const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware para parsear JSON
app.use(bodyParser.json());
app.use(express.json());

// Configuración de la conexión a la base de datos
const connection = mysql.createConnection({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: process.env.NAME
});

// Conexión a la base de datos
connection.connect((err) => {
  if (err) {
    console.error('Error al conectar a la base de datos:', err);
    return;
  }
  console.log('Conectado a la base de datos MySQL.');
});



// Ruta de prueba
app.get('/', (req, res) => {
  res.send('El backend está funcionando.');
});

// Ruta para obtener usuarios
app.get('/api/users', (req, res) => {
    connection.query('SELECT * FROM user_account', (err, results) => {
      if (err) {
        console.error('Error al obtener usuarios:', err);
        res.status(500).json({ error: 'Error al obtener usuarios.' });
        return;
      }
      res.json(results);
    });
  });

// Ruta para crear un nuevo usuario
app.post('/api/users', (req, res) => {
  const { email, password } = req.body;
  const query = 'INSERT INTO user_account (email, password) VALUES (?, ?)';
  const values = [email, password];
  connection.query(query, values, (err, results) => {
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