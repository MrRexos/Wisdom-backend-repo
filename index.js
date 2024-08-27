const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
const bcrypt = require('bcrypt');

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
    // Hashear la contraseña antes de guardarla en la base de datos
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    const query = 'INSERT INTO user_account (email, username, password, first_name, surname, joined_datetime, language, allow_notis) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?)';
    const values = [email, username, hashedPassword, first_name, surname, language, allow_notis];

    // Ejecutar la consulta para insertar el nuevo usuario
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

  // Buscar al usuario en la base de datos por nombre de usuario o correo electrónico
  const query = 'SELECT * FROM user_account WHERE username = ? OR email = ?';
  pool.query(query, [usernameOrEmail, usernameOrEmail], async (err, results) => {
    if (err) {
      console.error('Error al iniciar sesión:', err);
      res.status(500).json({ error: 'Error al iniciar sesión.' });
      return;
    }
    if (results.length > 0) {
      const user = results[0];
      
      // Comparar la contraseña proporcionada con el hash almacenado en la base de datos
      try {
        const match = await bcrypt.compare(password, user.password);
        if (match) {
          res.json({ success: true, message: 'Inicio de sesión exitoso.' });
        } else {
          res.status(401).json({ success: false, message: 'Credenciales incorrectas.', user.password });
        }
      } catch (error) {
        console.error('Error al comparar la contraseña:', error);
        res.status(500).json({ error: 'Error al procesar la solicitud.' });
      }
    } else {
      res.status(401).json({ success: false, message: 'Credenciales incorrectas.' });
    }
  });
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});
