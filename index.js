const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
const bcrypt = require('bcrypt');
const { Storage } = require('@google-cloud/storage');
const multer = require('multer');
const path = require('path');

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
  connectionLimit: 100,  // Número máximo de conexiones en el pool
  queueLimit: 0
});

const credentials = JSON.parse(process.env.GCLOUD_KEYFILE_JSON);

// Configura el almacenamiento de Google Cloud
const storage = new Storage({
  projectId: credentials.project_id,
  credentials: credentials,
});

const bucket = storage.bucket(process.env.GCLOUD_BUCKET_NAME);

// Configura Multer para manejar la subida de archivos
const multerMid = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 5 * 1024 * 1024, // Límite de 5MB por archivo
  },
});

app.use(multerMid.single('file'));

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
app.post('/api/signup', async (req, res) => {
  const { email, username, password, first_name, surname, language, allow_notis } = req.body;

  try {
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    const query = 'INSERT INTO user_account (email, username, password, first_name, surname, joined_datetime, language, allow_notis) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?)';
    const values = [email, username, hashedPassword, first_name, surname, language, allow_notis];

    pool.query(query, values, (err, results) => {
      if (err) {
        console.error('Error al crear el usuario:', err);
        res.status(500).send('Error al crear el usuario.');
        return;
      }
      res.status(201).send('Usuario creado.');
    });
  } catch (err) {
    console.error('Error al hashear la contraseña:', err);
    res.status(500).send('Error al procesar la solicitud.');
  }
});

// Ruta para verificar si un email ya existe
app.get('/api/check-email', (req, res) => {
  const { email } = req.query;
  const query = 'SELECT COUNT(*) AS count FROM user_account WHERE email = ?';
  pool.query(query, [email], (err, results) => {
    if (err) {
      console.error('Error al verificar el email:', err);
      res.status(500).json({ error: 'Error al verificar el email.' });
      return;
    }
    const count = results[0].count;
    res.json({ exists: count > 0 });
  });
});

// Ruta para verificar si un usuario ya existe
app.get('/api/check-username', (req, res) => {
  const { username } = req.query;
  const query = 'SELECT COUNT(*) AS count FROM user_account WHERE username = ?';
  pool.query(query, [username], (err, results) => {
    if (err) {
      console.error('Error al verificar el nombre de usuario:', err);
      res.status(500).json({ error: 'Error al verificar el nombre de usuario.' });
      return;
    }
    const count = results[0].count;
    res.json({ exists: count > 0 });
  });
});

// Ruta para hacer login
app.post('/api/login', (req, res) => {
  const { usernameOrEmail, password } = req.body;

  const query = 'SELECT * FROM user_account WHERE username = ? OR email = ?';
  pool.query(query, [usernameOrEmail, usernameOrEmail], async (err, results) => {
    if (err) {
      console.error('Error al iniciar sesión:', err);
      res.status(500).json({ error: 'Error al iniciar sesión.' });
      return;
    }
    if (results.length > 0) {
      const user = results[0];
      try {
        const match = await bcrypt.compare(password, user.password);
        if (match) {
          delete user.password;
          res.json({ success: true, message: 'Inicio de sesión exitoso.', user });
        } else {
          res.json({ success: false, message: 'Password incorrect.' });
        }
      } catch (error) {
        console.error('Error al comparar la contraseña:', error);
        res.status(500).json({ error: 'Error al procesar la solicitud.' });
      }
    } else {
      res.json({ success: null, message: "Wrong user or password." });
    }
  });
});

// Nueva ruta para subir imágenes a Google Cloud Storage
app.post('/upload', async (req, res, next) => {
  try {
    if (!req.file) {
      res.status(400).send('No se subió ningún archivo.');
      return;
    }

    const blob = bucket.file(req.file.originalname);
    const blobStream = blob.createWriteStream({
      resumable: false,
    });

    blobStream.on('error', err => {
      next(err);
    });

    blobStream.on('finish', () => {
      const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`;
      res.status(200).send({ url: publicUrl });
    });

    blobStream.end(req.file.buffer);
  } catch (error) {
    res.status(500).send(error);
  }
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});
