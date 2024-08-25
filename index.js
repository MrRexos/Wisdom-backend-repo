import express from 'express';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
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

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware para parsear JSON
app.use(bodyParser.json());
app.use(express.json());

// Ruta de prueba
app.get('/', (req, res) => {
  res.send('El backend está funcionando.');
});

// Ruta para obtener usuarios
app.get('/api/users', async (req, res) => {
  const promise = db.promise();
  const selectUserQuery = 'SELECT * FROM user_account';

  try {
    const [rows, fields] = await promise.execute(selectUserQuery);
    res.status(200).json({ users: rows });
  } catch (err) {
    console.error('Error al obtener usuarios:', err);
    res.status(500).json({ error: 'Error al obtener usuarios.' });
  }
});

// Ruta para crear un nuevo usuario
app.post('/api/users', async (req, res) => {
  const { first_name, last_name, username, email, password, profile_picture, language, allowNotis } = req.body;
  const promise = db.promise();
  const insertUserQuery = `
    INSERT INTO user_account (first_name, last_name, username, email, password, profile_picture, language, allowNotis)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
  const values = [first_name, last_name, username, email, password, profile_picture, language, allowNotis];

  try {
    const [results, fields] = await promise.execute(insertUserQuery, values);
    res.status(201).send('Usuario creado.');
  } catch (err) {
    console.error('Error al crear el usuario:', err);
    res.status(500).send('Error al crear el usuario.');
  }
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});
