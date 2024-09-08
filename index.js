const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
const bcrypt = require('bcrypt');
const { Storage } = require('@google-cloud/storage');
const multer = require('multer');
const path = require('path');
const sharp = require('sharp');

require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(express.json());

const pool = mysql.createPool({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: process.env.DATABASE,
  waitForConnections: true,
  connectionLimit: 10,  // Reducido para evitar superar el límite de Heroku
  acquireTimeout: 10000,
  idleTimeout: 60000,
});

const credentials = JSON.parse(process.env.GCLOUD_KEYFILE_JSON);

const storage = new Storage({
  projectId: credentials.project_id,
  credentials: credentials,
});

const bucket = storage.bucket(process.env.GCLOUD_BUCKET_NAME);

const multerMid = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
  },
});

app.use(multerMid.single('file'));

app.get('/', (req, res) => {
  res.send('El backend está funcionando.');
});

// Ruta para obtener usuarios
app.get('/api/users', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    try {
      connection.query('SELECT * FROM user_account', (err, results) => {
        if (err) {
          console.error('Error al obtener usuarios:', err);
          res.status(500).json({ error: 'Error al obtener usuarios.' });
          return;
        }
        res.json(results);
      });
    } finally {
      connection.release();
    }
  });
});

// Ruta para crear un nuevo usuario
app.post('/api/signup', async (req, res) => {
  const { email, username, password, first_name, surname, language, allow_notis, profile_picture } = req.body;

  try {
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);
    const query = 'INSERT INTO user_account (email, username, password, first_name, surname, joined_datetime, language, allow_notis, profile_picture) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?)';
    const values = [email, username, hashedPassword, first_name, surname, language, allow_notis, profile_picture];

    pool.getConnection((err, connection) => {
      if (err) {
        console.error('Error al obtener la conexión:', err);
        res.status(500).json({ error: 'Error al obtener la conexión.' });
        return;
      }

      try {
        connection.query(query, values, (err, results) => {
          if (err) {
            console.error('Error al crear el usuario:', err);
            res.status(500).send('Error al crear el usuario.');
            return;
          }
          res.status(201).send('Usuario creado.');
        });
      } finally {
        connection.release();
      }
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

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    try {
      connection.query(query, [email], (err, results) => {
        if (err) {
          console.error('Error al verificar el email:', err);
          res.status(500).json({ error: 'Error al verificar el email.' });
          return;
        }
        const count = results[0].count;
        res.json({ exists: count > 0 });
      });
    } finally {
      connection.release();
    }
  });
});

// Ruta para verificar si un usuario ya existe
app.get('/api/check-username', (req, res) => {
  const { username } = req.query;
  const query = 'SELECT COUNT(*) AS count FROM user_account WHERE username = ?';

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    try {
      connection.query(query, [username], (err, results) => {
        if (err) {
          console.error('Error al verificar el nombre de usuario:', err);
          res.status(500).json({ error: 'Error al verificar el nombre de usuario.' });
          return;
        }
        const count = results[0].count;
        res.json({ exists: count > 0 });
      });
    } finally {
      connection.release();
    }
  });
});

// Ruta para hacer login
app.post('/api/login', (req, res) => {
  const { usernameOrEmail, password } = req.body;
  const query = 'SELECT * FROM user_account WHERE username = ? OR email = ?';

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    try {
      connection.query(query, [usernameOrEmail, usernameOrEmail], async (err, results) => {
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
          res.json({ success: null, message: 'Wrong user or password.' });
        }
      });
    } finally {
      connection.release();
    }
  });
});

// Nueva ruta para subir imágenes a Google Cloud Storage
app.post('/api/upload-image', async (req, res, next) => {
  console.log('Archivo recibido:', req.file);
  try {
    if (!req.file) {
      res.status(400).send('No se subió ningún archivo.');
      return;
    }

    const image = sharp(req.file.buffer);
    const metadata = await image.metadata();
    let format = metadata.format;

    let compressedImage;
    if (['jpeg', 'jpg'].includes(format)) {
      compressedImage = await image.resize({ width: 800 }).jpeg({ quality: 80 }).toBuffer();
    } else if (format === 'png') {
      compressedImage = await image.resize({ width: 800 }).png({ quality: 60 }).toBuffer();
    } else if (format === 'webp') {
      compressedImage = await image.resize({ width: 800 }).webp({ quality: 60 }).toBuffer();
    } else if (format === 'heif') {
      compressedImage = await image.resize({ width: 800 }).tiff({ quality: 60 }).toBuffer();
    } else {
      res.status(415).send('Formato de archivo no soportado.');
      return;
    }

    const blob = bucket.file(req.file.originalname);
    const blobStream = blob.createWriteStream();

    blobStream.on('error', err => {
      next(err);
    });

    blobStream.on('finish', () => {
      const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`;
      res.status(200).send({ url: publicUrl });
    });

    blobStream.end(compressedImage);
  } catch (error) {
    res.status(500).send(error);
  }
});

app.get('/api/user/:userId/lists', (req, res) => {
  const { userId } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    try {
      connection.query('SELECT id, list_name FROM service_list WHERE user_id = ?', [userId], (err, lists) => {
        if (err) {
          console.error('Error al obtener las listas:', err);
          res.status(500).json({ error: 'Error al obtener las listas.' });
          return;
        }

        const listsWithItems = [];
        lists.forEach((list, index) => {
          connection.query('SELECT COUNT(*) AS itemCount FROM item_list WHERE list_id = ?', [list.id], (err, countResults) => {
            if (err) {
              console.error('Error al obtener el número de items:', err);
              res.status(500).json({ error: 'Error al obtener el número de items.' });
              return;
            }

            connection.query('SELECT added_datetime FROM item_list WHERE list_id = ? ORDER BY added_datetime DESC LIMIT 1', [list.id], (err, lastItemResults) => {
              if (err) {
                console.error('Error al obtener la fecha del último item añadido:', err);
                res.status(500).json({ error: 'Error al obtener la fecha del último item añadido.' });
                return;
              }

              const lastAddedDatetime = lastItemResults.length > 0 ? lastItemResults[0].added_datetime : null;

              listsWithItems.push({
                id: list.id,
                list_name: list.list_name,
                itemCount: countResults[0].itemCount,
                lastAddedDatetime: lastAddedDatetime,
              });

              if (index === lists.length - 1) {
                res.json(listsWithItems);
              }
            });
          });
        });

        if (lists.length === 0) {
          res.json(listsWithItems); // Si no hay listas, devolver un array vacío
        }
      });
    } finally {
      connection.release();
    }
  });
});

app.listen(port, () => {
  console.log(`Servidor en funcionamiento en el puerto ${port}`);
});
