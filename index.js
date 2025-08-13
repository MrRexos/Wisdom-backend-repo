require('dotenv').config();

const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { Storage } = require('@google-cloud/storage');
const multer = require('multer');
const path = require('path');
const sharp = require('sharp');
const nodemailer = require('nodemailer');
const crypto = require("crypto");
const PDFDocument = require('pdfkit');
const fs = require('fs');
const os = require('os');

const Stripe = require('stripe');
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
async function handleStripeRollbackIfNeeded(error) {
  try {
    if (error && error.payment_intent) {
      const intentId = typeof error.payment_intent === "string" ? error.payment_intent : error.payment_intent.id;
      await stripe.paymentIntents.cancel(intentId);
    }
  } catch (cancelErr) {
    console.error("Error cancelling payment intent:", cancelErr);
  }
}


const app = express();
const port = process.env.PORT || 3000;

// Middleware para parsear JSON. 
app.use(bodyParser.json());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const upload = multer({ storage: multer.memoryStorage() });
const uploadDni = multer({
  dest: os.tmpdir(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'image/jpeg' || file.mimetype === 'image/png') {
      cb(null, true);
    } else {
      cb(new Error('INVALID_FILE_TYPE'));
    }
  },
});

// CIDs for email images
const wisdomLogoCid = 'wisdom_logo';
const instagramLogoCid = 'instagram_logo';
const twitterLogoCid = 'twitter_logo';

// Configuración de transporte para enviar correos.
const transporter = nodemailer.createTransport({
  host: process.env.EMAIL_HOST,
  port: process.env.EMAIL_PORT,
  secure: false,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
});

// Comprobar la configuración del transporte al iniciar la aplicación
transporter.verify().catch(err => {
  console.error('Nodemailer configuration error:', err);
});

// Middleware para verificar tokens JWT
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Token requerido' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Token inválido' });
    }
    req.user = user;
    next();
  });
}

//Formats dates and times in English (GB)
function formatDateTime(date) {
  return new Date(date).toLocaleString('en-GB', {
    dateStyle: 'medium',
    timeStyle: 'short'
  });
}

// Configuración del pool de conexiones a la base de datos a // JSON.parse(process.env.GOOGLE_CREDENTIALS)..
const pool = mysql.createPool({
  host: process.env.DB_HOST, //process.env.HOST process.env.USER process.env.PASSWORD process.env.DATABASE
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
  //socketPath: `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`,
  waitForConnections: true,
  connectionLimit: 20,  // Número máximo de conexiones en el pool
  acquireTimeout: 20000,  // Tiempo máximo para adquirir una conexión
  connectTimeout: 20000,     // Tiempo máximo que una conexión puede estar inactiva antes de ser liberada.
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
    fileSize: 10 * 1024 * 1024, // Límite de 10MB por archivo
  },
});



// Ruta de prueba
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

    connection.query('SELECT * FROM user_account', (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al obtener usuarios:', err);
        res.status(500).json({ error: 'Error al obtener usuarios.' });
        return;
      }
      res.json(results);
    });
  });
});

// Ruta para crear un nuevo usuario
app.post('/api/signup', async (req, res) => {
  const { email, username, password, first_name, surname, language, allow_notis, profile_picture, phone } = req.body;

  try {
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    const query = 'INSERT INTO user_account (email, username, password, first_name, surname, phone, joined_datetime, language, allow_notis, profile_picture, is_verified) VALUES (?, ?, ?, ?, ?, ?, NOW(), ?, ?, ?, 0)';
    const values = [email, username, hashedPassword, first_name, surname, phone, language, allow_notis, profile_picture];

    pool.getConnection((err, connection) => {
      if (err) {
        console.error('Error al obtener la conexión:', err);
        res.status(500).json({ error: 'Error al obtener la conexión.' });
        return;
      }

      // Inserta el nuevo usuario
      connection.query(query, values, (err, results) => {
        if (err) {
          connection.release(); // Libera la conexión
          console.error('Error al crear el usuario:', err);
          res.status(500).send('Error al crear el usuario.');
          return;
        }

        const userId = results.insertId; // ID del usuario recién creado

        // Inserta la lista "Recently seen" en la tabla service_list
        const serviceListQuery = 'INSERT INTO service_list (list_name, user_id) VALUES (?, ?)';
        const serviceListValues = ['Recently seen', userId];

        connection.query(serviceListQuery, serviceListValues, async (err) => {
          connection.release(); // Libera la conexión después de la segunda consulta

          if (err) {
            console.error('Error al crear la lista de servicios:', err);
            res.status(500).send('Error al crear la lista de servicios.');
            return;
          }
          const token = jwt.sign({ id: userId }, process.env.JWT_SECRET, { expiresIn: '1h' });

          // Enviar correo de verificación
          try {
            const verifyToken = jwt.sign({ id: userId }, process.env.JWT_SECRET, { expiresIn: '1d' });
            const url = `${process.env.BASE_URL}/api/verify-email?token=${verifyToken}`;
            await transporter.sendMail({
              from: '"Wisdom" <wisdom.helpcontact@gmail.com>', // process.env.EMAIL_USER,
              to: email,
              subject: 'Confirm your Wisdom',
              attachments: [
                { filename: 'wisdom.png', path: path.join(__dirname, 'assets', 'wisdom.png'), cid: wisdomLogoCid },
                { filename: 'instagram.png', path: path.join(__dirname, 'assets', 'instagram.png'), cid: instagramLogoCid },
                { filename: 'twitter.png', path: path.join(__dirname, 'assets', 'twitter.png'), cid: twitterLogoCid }
              ],
              html:`
              <table
                width="100%"
                cellpadding="0"
                cellspacing="0"
                style="background:#ffffff;font-family:Inter,sans-serif;color:#111827;"
              >
                <tr>
                  <td align="center" style="padding:48px 24px;">
                    <!-- LOGO -->
                    <div style="font-size:24px;font-weight:600;letter-spacing:.6px;margin-bottom:32px;">
                      WISDOM<sup style="font-size:12px;vertical-align:top;">®</sup>
                    </div>

                    <!-- TÍTULO -->
                    <h1 style="font-size:30px;font-weight:500;margin-bottom:16px;">
                      Welcome to Wisdom
                    </h1>

                    <!-- TEXTO -->
                    <p style="font-size:16px;line-height:1.55;max-width:420px;margin:0 auto 50px;">
                      You've successfully sign up on Wisdom. Please confirm your email.
                    </p>

                    <!-- BOTÓN (enlace) -->
                    <a
                      href="${url}"
                      style="
                        display:inline-block;
                        padding:22px 100px;
                        background:#f3f3f3;
                        border-radius:14px;
                        text-decoration:none;
                        font-size:14px;
                        font-weight:600;
                        color:#111827;
                      "
                    >
                      Verify email
                    </a>

                    <!-- LÍNEA DIVISORIA -->
                    <hr
                      style="
                        border:none;
                        height:1px;
                        background-color:#f3f4f6;
                        margin:70px 0;
                        width:100%;
                      "
                    />

                    <!-- SOCIAL ICONS -->
                    <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
                      <tr>
                        <td style="padding:0 5px;">
                          <a href="https://wisdom-web.vercel.app/" aria-label="Wisdom web"
                            style="
                              display: flex;
                              width: 32px;
                              height: 32px;
                              background: #f3f4f6;
                              border-radius: 50%;
                              text-decoration: none;
                              justify-content: center;
                              align-items: center;
                            ">
                            
                            <img src="cid:${wisdomLogoCid}" width="18" height="18"
                              alt="Wisdom"
                              style="display:block; margin:auto; max-width:18px; max-height:18px; object-fit:contain;" />

                          </a>
                        </td>
                        <td style="padding:0 5px;">
                          <a href="https://www.instagram.com/wisdom__app/" aria-label="Instagram"
                            style="
                              display: flex;
                              width: 32px;
                              height: 32px;
                              background: #f3f4f6;
                              border-radius: 50%;
                              text-decoration: none;
                              justify-content: center;
                              align-items: center;
                            ">
                            <img src="cid:${instagramLogoCid}" alt="Instagram" width="18" height="18" style="display:block;margin:auto;" />
                          </a>
                        </td>
                        <td style="padding:0 0px;">
                          <a href="https://x.com/wisdom_entity" aria-label="Twitter"
                            style="
                              display: flex;
                              width: 32px;
                              height: 32px;
                              background: #f3f4f6;
                              border-radius: 50%;
                              text-decoration: none;
                              justify-content: center;
                              align-items: center;
                            ">
                            <img src="cid:${twitterLogoCid}" alt="Twitter" width="18" height="18" style="display:block;margin:auto;" />
                          </a>
                        </td>
                      </tr>
                    </table>

                    <!-- PIE DE PÁGINA -->
                    <div style="font-size:12px;color:#6b7280;line-height:1.4;text-decoration:none;">
                      <a href="#" style="color:#6b7280;text-decoration:none;">Privacy Policy</a>
                      &nbsp;·&nbsp;
                      <a href="#" style="color:#6b7280;text-decoration:none;">Terms of Service</a>
                      <br /><br />
                      Mataró, BCN, 08304
                      <br /><br />
                      This email was sent to ${email}
                    </div>
                  </td>
                </tr>
              </table>
              `
            });
          } catch (mailErr) {
            console.error('Error al enviar el correo de verificación:', mailErr);
          }

          res.status(201).json({ message: 'Usuario y lista de servicios creados.', userId, token });
        });
      });
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

    connection.query(query, [email], (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al verificar el email:', err);
        res.status(500).json({ error: 'Error al verificar el email.' });
        return;
      }
      const count = results[0].count;
      res.json({ exists: count > 0 });
    });
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

    connection.query(query, [username], (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al verificar el nombre de usuario:', err);
        res.status(500).json({ error: 'Error al verificar el nombre de usuario.' });
        return;
      }
      const count = results[0].count;
      res.json({ exists: count > 0 });
    });
  });
});

// Ruta para verificar el correo electrónico
app.get('/api/verify-email', (req, res) => {
  const { token } = req.query;
  if (!token) {
    return res.status(400).send('Token requerido');
  }
  jwt.verify(token, process.env.JWT_SECRET, (err, decoded) => {
    if (err) {
      return res.status(400).send('Token inválido');
    }
    const userId = decoded.id;
    pool.getConnection((connErr, connection) => {
      if (connErr) {
        console.error('Error al obtener la conexión:', connErr);
        return res.status(500).send('Error de conexión');
      }
      connection.query('UPDATE user_account SET is_verified = 1 WHERE id = ?', [userId], (updErr) => {
        connection.release();
        if (updErr) {
          console.error('Error al verificar el usuario:', updErr);
          return res.status(500).send('Error al verificar el usuario');
        }
        res.sendFile(path.join(__dirname, 'public', 'verify-success.html'));
      });
    });
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

    connection.query(query, [usernameOrEmail, usernameOrEmail], async (err, results) => {
      connection.release(); // Libera la conexión después de usarla

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
            const token = jwt.sign({ id: user.id }, process.env.JWT_SECRET, { expiresIn: '1h' });
            res.json({ success: true, message: 'Inicio de sesión exitoso.', user, token });
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
});

// Enviar enlace para restablecer contraseña
app.post('/api/forgot-password', (req, res) => {

  const { emailOrUsername } = req.body;
  if (!emailOrUsername) {
    return res.status(400).json({ error: 'Email or username required' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'SELECT id, email FROM user_account WHERE email = ? OR username = ?';
    connection.query(query, [emailOrUsername, emailOrUsername], async (err, results) => {
      if (err) {
        connection.release();
        console.error('Error al buscar el usuario:', err);
        return res.status(500).json({ error: 'Error al buscar el usuario.' });
      }

      if (results.length === 0) {
        connection.release();
        return res.status(200).json({ notFound: true });
      }

      const { id, email } = results[0];
      const resetCode = Math.floor(100000 + Math.random() * 900000).toString();
      const expiresAt = new Date(Date.now() + 15 * 60 * 1000);

      connection.query(
        'REPLACE INTO password_reset_codes (user_id, code, expires_at) VALUES (?, ?, ?)',
        [id, resetCode, expiresAt],
        async (codeErr) => {
          connection.release();
          if (codeErr) {
            console.error('Error al guardar el código de restablecimiento:', codeErr);
            return res.status(500).json({ error: 'Error al generar el código.' });
          }

          try {
            await transporter.sendMail({
              from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
              to: email,
              subject: 'Reset your password for Wisdom',
              attachments: [
                { filename: 'wisdom.png', path: path.join(__dirname, 'assets', 'wisdom.png'), cid: wisdomLogoCid },
                { filename: 'instagram.png', path: path.join(__dirname, 'assets', 'instagram.png'), cid: instagramLogoCid },
                { filename: 'twitter.png', path: path.join(__dirname, 'assets', 'twitter.png'), cid: twitterLogoCid }
              ],
              html: `
                <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;font-family:Inter,sans-serif;color:#111827;">
                  <tr>
                    <td align="center" style="padding:48px 24px;">
                      <div style="font-size:24px;font-weight:600;letter-spacing:.6px;margin-bottom:32px;">
                        WISDOM<sup style="font-size:12px;vertical-align:top;">®</sup>
                      </div>
                      <p style="font-size:16px;line-height:1.55;max-width:420px;margin:0 auto 50px;">
                        It looks like you lost your password. Use the code below to reset it.
                      </p>
                      <div style="font-size:30px;font-weight:600;margin-bottom:32px;">${resetCode}</div>
                      <hr style="border:none;height:1px;background-color:#f3f4f6;margin:70px 0;width:100%;" />
                      <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
                        <tr>
                          <td style="padding:0 5px;">
                            <a href="https://wisdom-web.vercel.app/" aria-label="Wisdom web" style="display:flex;width:32px;height:32px;background:#f3f4f6;border-radius:50%;text-decoration:none;justify-content:center;align-items:center;">
                              <img src="cid:${wisdomLogoCid}" width="18" height="18" alt="Wisdom" style="display:block;margin:auto;max-width:18px;max-height:18px;object-fit:contain;" />
                            </a>
                          </td>
                          <td style="padding:0 5px;">
                            <a href="https://www.instagram.com/wisdom__app/" aria-label="Instagram" style="display:flex;width:32px;height:32px;background:#f3f4f6;border-radius:50%;text-decoration:none;justify-content:center;align-items:center;">
                              <img src="cid:${instagramLogoCid}" alt="Instagram" width="18" height="18" style="display:block;margin:auto;" />
                            </a>
                          </td>
                          <td style="padding:0 0px;">
                            <a href="https://x.com/wisdom_entity" aria-label="Twitter" style="display:flex;width:32px;height:32px;background:#f3f4f6;border-radius:50%;text-decoration:none;justify-content:center;align-items:center;">
                              <img src="cid:${twitterLogoCid}" alt="Twitter" width="18" height="18" style="display:block;margin:auto;" />
                            </a>
                          </td>
                        </tr>
                      </table>
                      <div style="font-size:12px;color:#6b7280;line-height:1.4;text-decoration:none;">
                        <a href="#" style="color:#6b7280;text-decoration:none;">Privacy Policy</a>
                        &nbsp;·&nbsp;
                        <a href="#" style="color:#6b7280;text-decoration:none;">Terms of Service</a>
                        <br /><br />
                        Mataró, BCN, 08304
                        <br /><br />
                        This email was sent to ${email}
                      </div>
                    </td>
                  </tr>
                </table>
                `
            });
          } catch (mailErr) {
            console.error('Error al enviar el correo de restablecimiento:', mailErr);
          }

          res.json({ message: 'Reset code sent' });
        }
      );
    }); 
  });
});

// Restablecer contraseña con token
app.post('/api/reset-password', async (req, res) => {
  const { emailOrUsername, code, newPassword } = req.body;
  if (!emailOrUsername || !code || !newPassword) {
    return res.status(400).json({ error: 'Code, user and new password required' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const queryUser = 'SELECT id FROM user_account WHERE email = ? OR username = ?';
    connection.query(queryUser, [emailOrUsername, emailOrUsername], async (userErr, userRes) => {
      if (userErr) {
        connection.release();
        console.error('Error al buscar el usuario:', userErr);
        return res.status(500).json({ error: 'Error al buscar el usuario.' });
      }

      if (userRes.length === 0) {
        connection.release();
        return res.status(400).json({ error: 'User not found' });
      }

      const userId = userRes[0].id;
      connection.query('SELECT code, expires_at FROM password_reset_codes WHERE user_id = ?', [userId], async (codeErr, codeRes) => {
        if (codeErr) {
          connection.release();
          console.error('Error al obtener el código:', codeErr);
          return res.status(500).json({ error: 'Error al verificar el código.' });
        }

        if (codeRes.length === 0) {
          connection.release();
          return res.status(400).json({ error: 'Invalid code' });
        }

        const record = codeRes[0];
        if (record.code !== code || new Date(record.expires_at) < new Date()) {
          connection.release();
          return res.status(400).json({ error: 'Invalid or expired code' });
        }

        try {
          const hashed = await bcrypt.hash(newPassword, 10);
          connection.query('UPDATE user_account SET password = ? WHERE id = ?', [hashed, userId], (updErr) => {
            if (updErr) {
              connection.release();
              console.error('Error al actualizar la contraseña:', updErr);
              return res.status(500).json({ error: 'Error al actualizar la contraseña.' });
            }

            connection.query('DELETE FROM password_reset_codes WHERE user_id = ?', [userId], (delErr) => {
              if (delErr) {
                connection.release();
                console.error('Error al eliminar el código de restablecimiento:', delErr);
                return res.status(500).json({ error: 'Error interno' });
              }

              connection.query('SELECT id, email, username, first_name, surname, phone, profile_picture, is_professional, language FROM user_account WHERE id = ?', [userId], (selErr, results) => {
                connection.release();
                if (selErr || results.length === 0) {
                  return res.status(500).json({ error: 'Error al obtener el usuario.' });
                }

                const user = results[0];
                const loginToken = jwt.sign({ id: userId }, process.env.JWT_SECRET, { expiresIn: '1h' });
                res.json({ message: 'Password reset successfully', user, token: loginToken });
              });
            });
          });
        } catch (hashErr) {
          connection.release();
          console.error('Error al hashear la nueva contraseña:', hashErr);
          res.status(500).json({ error: 'Error al procesar la solicitud.' });
        }
      });
    });
  });
});




// Proteger las rutas siguientes con JWT
app.use(authenticateToken);

// Nueva ruta para subir imágenes a Google Cloud Storage
app.post('/api/upload-image', multerMid.single('file'), async (req, res, next) => {

  console.log('Archivo recibido:', req.file);

  try {

    if (!req.file) {
      res.status(400).send('No se subió ningún archivo.');
      return;
    }
    
    // Detecta el formato de la imagen
    const image = sharp(req.file.buffer);
    const metadata = await image.metadata();
    let format = metadata.format;

    // Procesa la imagen según el formato
    let compressedImage;
    if (format === 'jpeg' || format === 'jpg') {
      compressedImage = await image
        .resize({ width: 800 })  // Ajusta el tamaño si es necesario
        .jpeg({ quality: 80 })   // Comprime la imagen JPEG
        .toBuffer();
    } else if (format === 'png') {
      compressedImage = await image
        .resize({ width: 800 })
        .png({ quality: 60 })    // Comprime la imagen PNG
        .toBuffer();
    } else if (format === 'webp') {
      compressedImage = await image
        .resize({ width: 800 })
        .webp({ quality: 60 })   // Comprime la imagen WebP
        .toBuffer();
    } else if (format === 'heif') {
      compressedImage = await image
        .resize({ width: 800 })
        .tiff({ quality: 60 })   // Comprime la imagen HEIC
        .toBuffer();
    } else {
      // Si el formato no es compatible, puedes devolver un error
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

//Ruta para obtener las listas de un usuario en favorites
app.get('/api/user/:userId/lists', (req, res) => {
  const { userId } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Obtener las listas del usuario en service_list y las listas compartidas en shared_list
    let query = `
      SELECT id, list_name, 'owner' AS role FROM service_list WHERE user_id = ?
      UNION
      SELECT service_list.id, service_list.list_name, 'shared' AS role 
      FROM service_list
      JOIN shared_list ON service_list.id = shared_list.list_id
      WHERE shared_list.user_id = ?;
    `;

    connection.query(query, [userId, userId], (err, lists) => {
      if (err) {
        console.error('Error al obtener las listas:', err);
        res.status(500).json({ error: 'Error al obtener las listas.' });
        connection.release();  // Libera la conexión
        return;
      }

      // Iterar sobre las listas para obtener los detalles
      const listsWithDetailsPromises = lists.map(list => {
        return new Promise((resolve, reject) => {
          connection.query('SELECT COUNT(*) as item_count FROM item_list WHERE list_id = ?', [list.id], (err, itemCountResult) => {
            if (err) {
              return reject(err);
            }

            connection.query('SELECT MAX(added_datetime) as last_item_date FROM item_list WHERE list_id = ?', [list.id], (err, lastItemDateResult) => {
              if (err) {
                return reject(err);
              }

              // Obtener los tres primeros servicios (service_id) de la lista
              connection.query('SELECT service_id FROM item_list WHERE list_id = ? ORDER BY id LIMIT 3', [list.id], (err, services) => {
                if (err) {
                  return reject(err);
                }

                const servicesWithImagesPromises = services.map(service => {
                  return new Promise((resolve, reject) => {
                    // Obtener la primera imagen para cada service_id
                    connection.query('SELECT image_url FROM service_image WHERE service_id = ? ORDER BY `order` LIMIT 1', [service.service_id], (err, images) => {
                      if (err) {
                        return reject(err);
                      }

                      resolve({
                        service_id: service.service_id,
                        image_url: images.length > 0 ? images[0].image_url : null // Si no hay imagen, devuelve null
                      });
                    });
                  });
                });

                Promise.all(servicesWithImagesPromises)
                  .then(servicesWithImages => {
                    resolve({
                      id: list.id,
                      title: list.list_name,
                      role: list.role,  // Rol del usuario en la lista (propietario o compartido)
                      item_count: itemCountResult[0].item_count,
                      last_item_date: lastItemDateResult[0].last_item_date,
                      services: servicesWithImages
                    });
                  })
                  .catch(error => reject(error));
              });
            });
          });
        });
      });

      Promise.all(listsWithDetailsPromises)
        .then(listsWithDetails => {
          res.json(listsWithDetails);
        })
        .catch(error => {
          console.error('Error al obtener los detalles de las listas:', error);
          res.status(500).json({ error: 'Error al obtener los detalles de las listas.' });
        })
        .finally(() => {
          connection.release();  // Libera la conexión
        });
    });
  });
});

//Ruta para actulizar el nombre de una lista
app.put('/api/list/:listId', (req, res) => {
  const { listId } = req.params;
  const { newName } = req.body;

  if (!newName) {
    return res.status(400).json({ error: 'El nuevo nombre de la lista es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Actualizar el nombre de la lista
    connection.query('UPDATE service_list SET list_name = ? WHERE id = ?', [newName, listId], (err, result) => {
      if (err) {
        console.error('Error al actualizar el nombre de la lista:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al actualizar el nombre de la lista.' });
      }

      if (result.affectedRows === 0) {
        connection.release(); // Libera la conexión
        return res.status(404).json({ error: 'Lista no encontrada.' });
      }

      res.json({ message: 'Nombre de la lista actualizado con éxito.' });
      connection.release(); // Libera la conexión
    });
  });
});

// Ruta para borrar una lista desde list
app.delete('/api/list/:listId', (req, res) => {
  const { listId } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Eliminar la lista
    connection.query('DELETE FROM service_list WHERE id = ?', [listId], (err, result) => {
      if (err) {
        console.error('Error al eliminar la lista:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al eliminar la lista.' });
      }

      if (result.affectedRows === 0) {
        connection.release(); // Libera la conexión
        return res.status(404).json({ error: 'Lista no encontrada.' });
      }

      // Opcional: eliminar los items asociados a la lista
      connection.query('DELETE FROM item_list WHERE list_id = ?', [listId], (err) => {
        if (err) {
          console.error('Error al eliminar los items de la lista:', err);
        }
        connection.release(); // Libera la conexión
      });

      res.json({ message: 'Lista eliminada con éxito.' });
    });
  });
});

//Ruta para compartir una lista
app.post('/api/list/share', (req, res) => {
  const { listId, user, permissions } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Verificar si el usuario existe y obtener su ID
    const getUserIdQuery = 'SELECT id FROM user_account WHERE username = ? OR email = ?';
    connection.query(getUserIdQuery, [user, user], (err, results) => {
      if (err) {
        console.error('Error al consultar el usuario:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al consultar el usuario.' });
      }

      if (results.length === 0) {
        connection.release(); // Libera la conexión
        return res.status(201).json({ notFound:true });
      }

      const userId = results[0].id;

      // Insertar una nueva fila en shared_list
      const insertQuery = 'INSERT INTO shared_list (list_id, user_id, permissions) VALUES (?, ?, ?)';
      connection.query(insertQuery, [listId, userId, permissions], (err, result) => {
        if (err) {
          console.error('Error al añadir el usuario a la lista compartida:', err);
          connection.release(); // Libera la conexión
          return res.status(500).json({ error: 'Error al añadir el usuario a la lista compartida.' });
        }

        res.status(201).json({ message: 'Usuario añadido a la lista compartida con éxito.' });
        connection.release(); // Libera la conexión
      });
    });
  });
});

// Ruta para obtener todos los items de una lista por su ID
app.get('/api/lists/:id/items', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Usar una sola consulta con JOIN para obtener los ítems y datos adicionales de la tabla service, price, user_account y review
    let query = `
      SELECT
        item_list.id AS item_id, 
        item_list.list_id, 
        item_list.service_id, 
        item_list.note, 
        item_list.order, 
        item_list.added_datetime,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.password,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.joined_datetime,
        user_account.is_professional,
        user_account.language,
        user_account.allow_notis,
        user_account.currency,
        user_account.money_in_wallet,
        user_account.professional_started_datetime,
        user_account.is_expert,
        user_account.is_verified,
        user_account.strikes_num,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating
      FROM item_list
      JOIN service ON item_list.service_id = service.id
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE item_list.list_id = ?;
    `;


    connection.query(query, [id], (err, itemsWithService) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener los ítems de la lista:', err);
        res.status(500).json({ error: 'Error al obtener los ítems de la lista.' });
        return;
      }

      if (itemsWithService.length > 0) {
        res.status(200).json(itemsWithService);
      } else {
        res.status(200).json({empty: true,  message: 'No se encontraron ítems para esta lista.' });
      }
    });
  });
});

//Ruta para añadir una nota
app.put('/api/items/:id/note', (req, res) => {
  const { id } = req.params;
  const { note } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para actualizar la columna 'note' en la tabla 'item_list'
    const query = `
      UPDATE item_list
      SET note = ?
      WHERE id = ?
    `;

    connection.query(query, [note, id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar la nota del ítem:', err);
        res.status(500).json({ error: 'Error al actualizar la nota del ítem.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Nota actualizada con éxito.' });
      } else {
        res.status(404).json({ message: 'Ítem no encontrado.' });
      }
    });
  });
});

// Ruta para borrar una lista desde favorites
app.delete('/api/lists/:id', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.query('DELETE FROM service_list WHERE id = ?', [id], (err, result) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la lista:', err);
        res.status(500).json({ error: 'Error al eliminar la lista.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Lista eliminada con éxito' });
      } else {
        res.status(404).json({ message: 'Lista no encontrada' });
      }
    });
  });
});

//Ruta para obtener todas las familias
app.get('/api/service-family', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener todos los registros de la tabla 'service_family'
    const query = 'SELECT * FROM service_family';

    connection.query(query, (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener los valores de la tabla service_family:', err);
        res.status(500).json({ error: 'Error al obtener los valores.' });
        return;
      }

      res.status(200).json(results); // Devolver los resultados
    });
  });
});

//Ruta para obtener todas las categorias de una lista a partir de la id de la lista
app.get('/api/service-family/:id/categories', (req, res) => {
  const { id } = req.params; // ID del service_family

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener las categorías asociadas a un service_family
    const query = `
      SELECT sc.id AS service_category_id, sct.id AS service_category_type_id, sct.service_category_name, sct.description
      FROM service_category sc
      JOIN service_category_type sct ON sc.service_category_type_id = sct.id
      WHERE sc.service_family_id = ?
    `;

    connection.query(query, [id], (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener las categorías:', err);
        res.status(500).json({ error: 'Error al obtener las categorías.' });
        return;
      }

      res.status(200).json(results); // Devolver las categorías
    });
  });
});

//Ruta para mostrar todos los servicios de una categoria
app.get('/api/category/:id/services', (req, res) => {
  const { id } = req.params; // ID de la categoría

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todos los servicios, sus tags y las imágenes
    const query = `
      SELECT 
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
         FROM service_tags 
         WHERE service_tags.service_id = service.id) AS tags,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.service_category_id = ?;
    `;

    connection.query(query, [id], (err, servicesData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de los servicios:', err);
        res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
        return;
      }

      if (servicesData.length > 0) {
        res.status(200).json(servicesData); // Devolver la lista de servicios con tags e imágenes
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron servicios para esta categoría.' });
      }
    });
  });
});

//Ruta para subir varias fotos (create service)
app.post('/api/upload-images', upload.array('files'), async (req, res, next) => {


  try {
      const results = await Promise.all(req.files.map(async (file, index) => {
          const image = sharp(file.buffer);
          const metadata = await image.metadata();
          let compressedImage;

          if (metadata.format === 'jpeg' || metadata.format === 'jpg') {
              compressedImage = await image
                  .resize({ width: 800 })
                  .jpeg({ quality: 80 })
                  .toBuffer();
          } else if (metadata.format === 'png') {
              compressedImage = await image
                  .resize({ width: 800 })
                  .png({ quality: 60 })
                  .toBuffer();
          } else if (metadata.format === 'webp') {
              compressedImage = await image
                  .resize({ width: 800 })
                  .webp({ quality: 60 })
                  .toBuffer();
          } else if (metadata.format === 'heif') {
              compressedImage = await image
                  .resize({ width: 800 })
                  .toBuffer();  // Usar toBuffer() para HEIF si no soporta compresión
          } else {
              throw new Error('Formato de archivo no soportado.');
          }

          const blob = bucket.file(`${Date.now()}_${file.originalname}`);
          const blobStream = blob.createWriteStream();

          return new Promise((resolve, reject) => {
              blobStream.on('error', reject);
              blobStream.on('finish', () => {
                  const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`;
                  resolve({ url: publicUrl, order: index + 1 });
              });
              blobStream.end(compressedImage);
          });
      }));

      res.status(200).send(results);
  } catch (error) {
      console.error('Error en la carga de imágenes:', error);
      res.status(500).send(error.message);
  }
});

//Ruta para crear un servicio
app.post('/api/service', (req, res) => {
  const {
    service_title,
    user_id,
    description,
    service_category_id,
    price,
    price_type,
    latitude,
    longitude,
    action_rate,
    user_can_ask,
    user_can_consult,
    price_consult,
    consult_via_provide,
    consult_via_username,
    consult_via_url,
    is_individual,
    allow_discounts,
    discount_rate,
    languages,     
    tags,           
    experiences,   
    images,         
    hobbies
  } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.beginTransaction(err => {
      if (err) {
        console.error('Error al iniciar la transacción:', err);
        connection.release(); // Liberar conexión en caso de error
        res.status(500).json({ error: 'Error al iniciar la transacción.' });
        return;
      }

      // 1. Insertar en la tabla 'price'
      const priceQuery = 'INSERT INTO price (price, price_type) VALUES (?, ?)';
      connection.query(priceQuery, [price, price_type], (err, result) => {
        if (err) {
          return connection.rollback(() => {
            console.error('Error al insertar en la tabla price:', err);
            connection.release(); // Liberar conexión en caso de error
            res.status(500).json({ error: 'Error al insertar en la tabla price.' });
          });
        }

        const price_id = result.insertId;

        // 2. Si user_can_consult es true, insertar en consult_via, de lo contrario, saltarlo.
        let consult_via_id = null;
        const insertService = () => {
          // 3. Insertar en la tabla 'service'
          const serviceQuery = `
            INSERT INTO service (
              service_title, user_id, description, service_category_id, price_id, latitude, longitude,
              action_rate, user_can_ask, user_can_consult, price_consult, consult_via_id, is_individual, allow_discounts, discount_rate, hobbies, service_created_datetime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
          `;
          const serviceValues = [
            service_title, user_id, description, service_category_id, price_id, latitude, longitude,
            action_rate, user_can_ask, user_can_consult, price_consult, consult_via_id, is_individual, allow_discounts, discount_rate, hobbies
          ];

          connection.query(serviceQuery, serviceValues, (err, result) => {
            if (err) {
              return connection.rollback(() => {
                console.error('Error al insertar en la tabla service:', err);
                connection.release(); // Liberar conexión en caso de error
                res.status(500).json({ error: 'Error al insertar en la tabla service.' });
              });
            }

            const service_id = result.insertId;

            // 4. Insertar lenguajes en 'service_language'
            if (languages && languages.length > 0) {
              const languageQuery = 'INSERT INTO service_language (service_id, language) VALUES ?';
              const languageValues = languages.map(lang => [service_id, lang]);

              connection.query(languageQuery, [languageValues], err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al insertar lenguajes:', err);
                    connection.release(); // Liberar conexión en caso de error
                    res.status(500).json({ error: 'Error al insertar lenguajes.' });
                  });
                }
              });
            }

            // 5. Insertar tags en 'service_tags'
            if (tags && tags.length > 0) {
              const tagsQuery = 'INSERT INTO service_tags (service_id, tag) VALUES ?';
              const tagsValues = tags.map(tag => [service_id, tag]);

              connection.query(tagsQuery, [tagsValues], err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al insertar tags:', err);
                    connection.release(); // Liberar conexión en caso de error
                    res.status(500).json({ error: 'Error al insertar tags.' });
                  });
                }
              });
            }

            // 6. Insertar experiencias en 'experience_place'
            if (experiences && experiences.length > 0) {
              const experienceQuery = 'INSERT INTO experience_place (service_id, experience_title, place_name, experience_started_date, experience_end_date) VALUES ?';
              const experienceValues = experiences.map(exp => [
                service_id,
                exp.experience_title,
                exp.place_name,
                new Date(exp.experience_started_date).toISOString().slice(0, 19).replace('T', ' '),
                exp.experience_end_date ? new Date(exp.experience_end_date).toISOString().slice(0, 19).replace('T', ' ') : null
              ]);

              connection.query(experienceQuery, [experienceValues], err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al insertar experiencias:', err);
                    console.error('Consulta:', experienceQuery);
                    console.error('Valores:', experienceValues);
                    connection.release(); // Liberar conexión en caso de error
                    res.status(500).json({ error: 'Error al insertar experiencias.' });
                  });
                } else {
                  console.log('Experiencias insertadas correctamente');
                }
              });
            } 

            // 7. Insertar imágenes en 'service_image'
            if (images && images.length > 0) {
              const imageQuery = 'INSERT INTO service_image (service_id, image_url, `order`) VALUES ?';
              const imageValues = images.map(img => [service_id, img.url, img.order]);

              connection.query(imageQuery, [imageValues], err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al insertar imágenes:', err);
                    connection.release(); // Liberar conexión en caso de error
                    res.status(500).json({ error: 'Error al insertar imágenes.' });
                  });
                }
              });
            }
            // 8. Marcar al usuario como profesional si aún no lo es
            const professionalQuery = 'UPDATE user_account SET is_professional = 1 WHERE id = ? AND is_professional = 0';
            connection.query(professionalQuery, [user_id], err => {
              if (err) {
                return connection.rollback(() => {
                  console.error('Error al actualizar el usuario como profesional:', err);
                  connection.release();
                  res.status(500).json({ error: 'Error al actualizar el usuario.' });
                });
              }

              // Commit final
              connection.commit(err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al hacer commit de la transacción:', err);
                    connection.release(); // Liberar conexión en caso de error
                    res.status(500).json({ error: 'Error al hacer commit de la transacción.' });
                  });
                }

                connection.release(); // Liberar conexión después del commit exitoso
                res.status(201).json({ message: 'Servicio creado con éxito.' });
              });
            });
          });
        };

        // 2.1. Insertar consult_via y continuar con insertService
        if (user_can_consult) {
          const consultViaQuery = 'INSERT INTO consult_via (provider, username, url) VALUES (?, ?, ?)';
          connection.query(consultViaQuery, [consult_via_provide, consult_via_username, consult_via_url], (err, result) => {
            if (err) {
              return connection.rollback(() => {
                console.error('Error al insertar en la tabla consult_via:', err);
                connection.release(); // Liberar conexión en caso de error
                res.status(500).json({ error: 'Error al insertar en la tabla consult_via.' });
              });
            }

            consult_via_id = result.insertId;
            insertService(); // Llama a insertService después de haber obtenido el consult_via_id
          });
        } else {
          insertService(); // Llama a insertService directamente si user_can_consult es false
        }
      });
    });
  });
});

//Ruta para crear lista
app.post('/api/lists', (req, res) => {
  const { user_id, list_name } = req.body;

  if (!user_id || !list_name) {
    return res.status(400).json({ error: 'user_id y list_name son requeridos.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'INSERT INTO service_list (user_id, list_name) VALUES (?, ?)';
    const values = [user_id, list_name];

    connection.query(query, values, (err, result) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al crear la lista:', err);
        return res.status(500).json({ error: 'Error al crear la lista.' });
      }

      res.status(201).json({ message: 'Lista creada con éxito', listId: result.insertId });
    });
  });
});

//Ruta para añadir un item a una lista
app.post('/api/lists/:list_id/items', (req, res) => {
  const { list_id } = req.params;
  const { service_id } = req.body;

  if (!service_id) {
    return res.status(400).json({ error: 'service_id es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Comprobar si ya existe un item con el mismo service_id en la lista
    const checkIfExistsQuery = 'SELECT id FROM item_list WHERE list_id = ? AND service_id = ?';
    connection.query(checkIfExistsQuery, [list_id, service_id], (err, results) => {
      if (err) {
        connection.release();
        console.error('Error al comprobar si el item ya existe:', err);
        return res.status(500).json({ error: 'Error al comprobar si el item ya existe.' });
      }

      // Si ya existe, no añadir el nuevo item
      if (results.length > 0) {
        connection.release();
        return res.status(201).json({ message: 'El item ya existe en la lista.', alreadyExists:true });
      }

      // Si no existe, proceder con la inserción
      const getLastOrderQuery = 'SELECT MAX(`order`) AS lastOrder FROM item_list WHERE list_id = ?';
      connection.query(getLastOrderQuery, [list_id], (err, result) => {
        if (err) {
          connection.release();
          console.error('Error al obtener el último orden:', err);
          return res.status(500).json({ error: 'Error al obtener el último orden.' });
        }

        const lastOrder = result[0].lastOrder || 0;
        const newOrder = lastOrder + 1;

        const insertItemQuery = `
          INSERT INTO item_list (list_id, service_id, \`order\`, added_datetime) 
          VALUES (?, ?, ?, NOW())
        `;
        const values = [list_id, service_id, newOrder];

        connection.query(insertItemQuery, values, (err, result) => {
          connection.release();

          if (err) {
            console.error('Error al añadir el item a la lista:', err);
            return res.status(500).json({ error: 'Error al añadir el item a la lista.' });
          }

          res.status(201).json({ message: 'Item añadido con éxito', itemId: result.insertId });
        });
      });
    });
  });
});

//Ruta para obtener toda la info de un servicio y mostrar su profile
app.get('/api/service/:id', (req, res) => {
  const { id } = req.params; // ID del servicio

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información del servicio y sus relaciones
    const query = `
      SELECT 
        s.id AS service_id,
        s.service_title,
        s.description,
        s.service_category_id,
        s.price_id,
        s.latitude,
        s.longitude,
        s.action_rate,
        s.user_can_ask,
        s.user_can_consult,
        s.price_consult,
        s.consult_via_id,
        s.is_individual,
        s.service_created_datetime,
        s.allow_discounts,
        s.discount_rate,
        s.hobbies,
        p.price,
        p.price_type,
        ua.id AS user_id,
        ua.email,
        ua.phone,
        ua.username,
        ua.first_name,
        ua.surname,
        ua.profile_picture,
        ua.is_professional,
        ua.language,
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
         FROM service_tags 
         WHERE service_id = s.id) AS tags,
        -- Subconsulta para obtener los idiomas del servicio
        (SELECT JSON_ARRAYAGG(language) 
         FROM service_language 
         WHERE service_id = s.id) AS languages,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = s.id) AS images,
        -- Subconsulta para obtener las reseñas del servicio con información del usuario
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', r.id, 
            'user_id', r.user_id, 
            'rating', r.rating, 
            'comment', r.comment, 
            'review_datetime', r.review_datetime,
            'user', JSON_OBJECT('id', ua_r.id,
                                'email', ua_r.email,
                                'phone', ua_r.phone,
                                'username', ua_r.username,
                                'first_name', ua_r.first_name,
                                'surname', ua_r.surname,
                                'profile_picture', ua_r.profile_picture))
         )
         FROM review r 
         JOIN user_account ua_r ON r.user_id = ua_r.id
         WHERE r.service_id = s.id) AS reviews,
        -- Calcular la media de valoraciones
        (SELECT AVG(r.rating) 
         FROM review r 
         WHERE r.service_id = s.id) AS average_rating,
        -- Contar el número total de reseñas
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id) AS review_count,
        -- Contar el número de reseñas por rating
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 5) AS rating_5_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 4) AS rating_4_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 3) AS rating_3_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 2) AS rating_2_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 1) AS rating_1_count,
        -- Subconsulta para obtener las experiencias del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', ep.id, 
            'experience_title', ep.experience_title, 
            'place_name', ep.place_name, 
            'experience_started_date', ep.experience_started_date, 
            'experience_end_date', ep.experience_end_date))
         FROM experience_place ep
         WHERE ep.service_id = s.id) AS experiences,
        -- Información de consult_via
        (SELECT JSON_OBJECT('id', cv.id, 'provider', cv.provider, 'username', cv.username, 'url', cv.url)
         FROM consult_via cv 
         WHERE cv.id = s.consult_via_id) AS consult_via,
        -- Información de la categoría del servicio
        (SELECT JSON_OBJECT('id', sc.id, 
          'name', sct.service_category_name, 
          'description', sct.description, 
          'family', JSON_OBJECT('id', sf.id, 'name', sf.service_family, 'description', sf.description))
         FROM service_category sc
         JOIN service_family sf ON sc.service_family_id = sf.id
         JOIN service_category_type sct ON sc.service_category_type_id = sct.id
         WHERE sc.id = s.service_category_id) AS category
      FROM service s
      JOIN price p ON s.price_id = p.id
      JOIN user_account ua ON s.user_id = ua.id
      WHERE s.id = ?;
    `;

    connection.query(query, [id], (err, serviceData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del servicio:', err);
        res.status(500).json({ error: 'Error al obtener la información del servicio.' });
        return;
      }

      if (serviceData.length > 0) {
        res.status(200).json(serviceData[0]); // Devolver la información del servicio
      } else {
        res.status(404).json({ notFound: true, message: 'Servicio no encontrado.' });
      }
    });
  });
});

//Ruta para obtener los 10 profesionales de la tabla
app.get('/api/suggested_professional', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener el ID del servicio y toda la información del usuario, con un límite de 20 resultados
    const query = `
      SELECT
        s.id AS service_id,
        s.service_title,
        ua.id AS user_id,
        ua.email,
        ua.phone,
        ua.username,
        ua.first_name,
        ua.surname,
        ua.profile_picture,
        ua.is_professional,
        ua.language
      FROM service s
      JOIN user_account ua ON s.user_id = ua.id
      LIMIT 20;
    `;

    connection.query(query, (err, servicesData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de los servicios:', err);
        res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
        return;
      }

      res.status(200).json(servicesData); // Devolver la información de hasta 20 servicios
    });
  });
});

//Ruta para obtener todas las reservas de un user
app.get('/api/user/:userId/bookings', (req, res) => {
  const { userId } = req.params; // ID del usuario
  const { status } = req.query;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas y servicios asociados
    let query = `
      SELECT
          booking.id AS booking_id,
          booking.booking_start_datetime,
          booking.booking_end_datetime,
          booking.service_duration,
          booking.final_price,
          booking.commission,
          booking.is_paid,
          booking.booking_status,
          booking.order_datetime,
          service.id AS service_id,
          service.service_title,
          service.description,
          service.service_category_id,
          service.price_id,
          service.latitude,
          service.longitude,
          service.action_rate,
          service.user_can_ask,
          service.user_can_consult,
          service.price_consult,
          service.consult_via_id,
          service.is_individual,
          service.service_created_datetime,
          price.price,
          price.price_type,
          user_account.id AS service_user_id,
          user_account.email,
          user_account.phone,
          user_account.username,
          user_account.first_name,
          user_account.surname,
          user_account.profile_picture,
          user_account.is_professional,
          user_account.language,
          (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
          FROM service_image si 
          WHERE si.service_id = service.id) AS images
      FROM booking
      LEFT JOIN service ON booking.service_id = service.id
      LEFT JOIN price ON service.price_id = price.id
      LEFT JOIN user_account ON service.user_id = user_account.id
      WHERE booking.user_id = ?`;

      const params = [userId];
      if (status) {
        query += ' AND booking.booking_status = ?';
        params.push(status);
      }
      query += ' ORDER BY booking.booking_start_datetime DESC;';
  
      connection.query(query, params, (err, bookingsData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de las reservas:', err);
        res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
        return;
      }

      if (bookingsData.length > 0) {
        res.status(200).json(bookingsData); // Devolver la lista de reservas con la información del servicio y usuario
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron reservas para este usuario.' });
      }
    });
  });
});

//Ruta para obtener todas las reservas de un profesional
app.get('/api/service-user/:userId/bookings', (req, res) => {
  const { userId } = req.params; // ID del usuario dentro de la tabla service
  const { status } = req.query;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas donde el servicio pertenece a un usuario específico
    let query = `
      SELECT 
        booking.id AS booking_id,
        booking.user_id AS booking_user_id,
        booking.booking_start_datetime,
        booking.booking_end_datetime,
        booking.service_duration,
        booking.final_price,
        booking.commission,
        booking.is_paid,
        booking.booking_status,
        booking.order_datetime,
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        -- Información del usuario que presta el servicio
        service_user.id AS service_user_id,
        service_user.email AS service_user_email,
        service_user.phone AS service_user_phone,
        service_user.username AS service_user_username,
        service_user.first_name AS service_user_first_name,
        service_user.surname AS service_user_surname,
        service_user.profile_picture AS service_user_profile_picture,
        service_user.is_professional AS service_user_is_professional,
        service_user.language AS service_user_language,
        -- Información del usuario que realizó la reserva
        booking_user.id AS booking_user_id,
        booking_user.email AS booking_user_email,
        booking_user.phone AS booking_user_phone,
        booking_user.username AS booking_user_username,
        booking_user.first_name AS booking_user_first_name,
        booking_user.surname AS booking_user_surname,
        booking_user.profile_picture AS booking_user_profile_picture,
        booking_user.is_professional AS booking_user_is_professional,
        booking_user.language AS booking_user_language,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = service.id) AS images
      FROM booking
      JOIN service ON booking.service_id = service.id
      JOIN price ON service.price_id = price.id
      JOIN user_account AS service_user ON service.user_id = service_user.id -- Usuario que presta el servicio
      JOIN user_account AS booking_user ON booking.user_id = booking_user.id -- Usuario que realizó la reserva
      WHERE service.user_id = ?`;

    const params = [userId];
    if (status) {
      query += ' AND booking.booking_status = ?';
      params.push(status);
    }
    query += ' ORDER BY booking.booking_start_datetime DESC;';

    connection.query(query, params, (err, bookingsData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de las reservas:', err);
        res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
        return;
      }

      if (bookingsData.length > 0) {
        res.status(200).json(bookingsData); // Devolver la lista de reservas con la información del servicio y usuario
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron reservas para este usuario.' });
      }
    });
  });
});

//Ruta para mostrar todos los servicios de un profesional
app.get('/api/user/:id/services', (req, res) => {
  const { id } = req.params; // ID del usuario

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todos los servicios, sus tags y las imágenes
    const query = `
      SELECT 
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
         FROM service_tags 
         WHERE service_tags.service_id = service.id) AS tags,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.user_id = ?;
    `;

    connection.query(query, [id], (err, servicesData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de los servicios:', err);
        res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
        return;
      }

      if (servicesData.length > 0) {
        res.status(200).json(servicesData); // Devolver la lista de servicios con tags e imágenes
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron servicios para este usuario.' });
      }
    });
  });
});

//Ruta para obtener el dinero en wallet
app.get('/api/user/:id/wallet', (req, res) => {
  const { id } = req.params; // ID del usuario

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener el valor de money_in_wallet
    const query = `
      SELECT money_in_wallet
      FROM user_account
      WHERE id = ?;
    `;

    connection.query(query, [id], (err, walletData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener el dinero en la cartera:', err);
        res.status(500).json({ error: 'Error al obtener el dinero en la cartera.' });
        return;
      }

      if (walletData.length > 0) {
        res.status(200).json({ money_in_wallet: walletData[0].money_in_wallet });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para obtener la información de un usuario
app.get('/api/user/:id', (req, res) => {
  const { id } = req.params; // ID del usuario

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      SELECT id, email, username, first_name, surname, phone, profile_picture,
             is_professional, language, joined_datetime
      FROM user_account
      WHERE id = ?;
    `;

    connection.query(query, [id], (err, userData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del usuario:', err);
        res.status(500).json({ error: 'Error al obtener la información del usuario.' });
        return;
      }

      if (userData.length > 0) {
        res.status(200).json(userData[0]);
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para actualizar el profile
app.put('/api/user/:id/profile', (req, res) => {
  const { id } = req.params; // ID del usuario
  const { profile_picture, username, first_name, surname, phone } = req.body; // Datos a actualizar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para actualizar los valores en user_account
    const query = `
      UPDATE user_account
      SET profile_picture = ?, username = ?, first_name = ?, surname = ?, phone = ?
      WHERE id = ?;
    `;

    connection.query(query, [profile_picture, username, first_name, surname, phone, id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar el perfil del usuario:', err);
        res.status(500).json({ error: 'Error al actualizar el perfil del usuario.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Perfil actualizado exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para actualizar account (ahora mismo solo actualiza email)
app.put('/api/user/:id/email', (req, res) => {
  const { id } = req.params; // ID del usuario
  const { email } = req.body; // Nuevo email a actualizar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para actualizar el email en user_account
    const query = `
      UPDATE user_account
      SET email = ?
      WHERE id = ?;
    `;

    connection.query(query, [email, id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar el email del usuario:', err);
        res.status(500).json({ error: 'Error al actualizar el email del usuario.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Email actualizado exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

// Cambiar contraseña 
app.put('/api/user/:id/password', authenticateToken, async (req, res) => {
  const { id } = req.params;
  const { currentPassword, newPassword } = req.body;

  if (parseInt(id, 10) !== req.user.id) {
    return res.status(403).json({ error: 'Acceso denegado' });
  }

  pool.getConnection(async (err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    connection.query('SELECT password FROM user_account WHERE id = ?', [id], async (err, results) => {
      if (err) {
        connection.release();
        console.error('Error al obtener la contraseña:', err);
        return res.status(500).json({ error: 'Error al obtener la contraseña.' });
      }

      if (results.length === 0) {
        connection.release();
        return res.status(404).json({ error: 'Usuario no encontrado.' });
      }

      const match = await bcrypt.compare(currentPassword, results[0].password);
      if (!match) {
        connection.release();
        return res.status(400).json({ error: 'Contraseña actual incorrecta.' });
      }

      const hashed = await bcrypt.hash(newPassword, 10);
      connection.query('UPDATE user_account SET password = ? WHERE id = ?', [hashed, id], (err) => {
        connection.release();
        if (err) {
          console.error('Error al actualizar la contraseña:', err);
          return res.status(500).json({ error: 'Error al actualizar la contraseña.' });
        }
        res.json({ message: 'Contraseña actualizada con éxito.' });
      });
    });
  });
});

//Ruta para borrar una cuenta
app.delete('/api/user/:id', (req, res) => {
  const { id } = req.params; // ID del usuario

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para eliminar el usuario de user_account
    const query = `
      DELETE FROM user_account
      WHERE id = ?;
    `;

    connection.query(query, [id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la cuenta del usuario:', err);
        res.status(500).json({ error: 'Error al eliminar la cuenta del usuario.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Cuenta eliminada exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para actualizar allow_notis de un usuario
app.put('/api/user/:id/allow_notis', (req, res) => {
  const { id } = req.params;  // ID del usuario
  const { allow_notis } = req.body;  // Nuevo valor de `allow_notis`

  // Verificar que el valor de `allow_notis` sea válido (booleano)
  if (typeof allow_notis !== 'boolean') {
      res.status(400).json({ error: 'El valor de allow_notis debe ser un booleano.' });
      return;
  }

  // Obtener una conexión del pool de MySQL
  pool.getConnection((err, connection) => {
      if (err) {
          console.error('Error al obtener la conexión:', err);
          res.status(500).json({ error: 'Error al obtener la conexión.' });
          return;
      }

      // Consulta SQL para actualizar el campo `allow_notis`
      const query = `
          UPDATE user_account
          SET allow_notis = ?
          WHERE id = ?;
      `;

      // Ejecutar la consulta
      connection.query(query, [allow_notis, id], (err, result) => {
          connection.release();  // Liberar la conexión después de usarla

          if (err) {
              console.error('Error al actualizar allow_notis:', err);
              res.status(500).json({ error: 'Error al actualizar allow_notis.' });
              return;
          }

          if (result.affectedRows > 0) {
              res.status(200).json({ message: 'allow_notis actualizado exitosamente.' });
          } else {
              res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
          }
      });
  });
});

// Ruta para añadir un strike a un usuario
app.post('/api/user/:id/strike', (req, res) => {
  const { id } = req.params; // ID del usuario

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      UPDATE user_account
      SET strikes_num = COALESCE(strikes_num, 0) + 1
      WHERE id = ?;
    `;

    connection.query(query, [id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al añadir el strike:', err);
        res.status(500).json({ error: 'Error al añadir el strike.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Strike añadido exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para guardar address + direction
app.post('/api/directions', (req, res) => {
  const { user_id, address_type, street_number, address_1, address_2, postal_code, city, state, country } = req.body;

  // Verificar que los campos requeridos estén presentes, excepto address_2 y street_number que pueden ser nulos
  if (!user_id || !address_type || !address_1 || !postal_code || !city || !state || !country) {
    return res.status(400).json({ error: 'Algunos campos requeridos faltan.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Primero insertar la dirección en la tabla address
    const addressQuery = 'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country];

    connection.query(addressQuery, addressValues, (err, result) => {
      if (err) {
        connection.release();
        console.error('Error al insertar la dirección:', err);
        return res.status(500).json({ error: 'Error al insertar la dirección.' });
      }

      const addressId = result.insertId; // Obtenemos el ID de la dirección recién insertada

      // Ahora insertar en la tabla directions utilizando el user_id y el address_id
      const directionsQuery = 'INSERT INTO directions (user_id, address_id) VALUES (?, ?)';
      const directionsValues = [user_id, addressId];

      connection.query(directionsQuery, directionsValues, (err, result) => {
        connection.release(); // Liberar la conexión después de usarla

        if (err) {
          console.error('Error al insertar la dirección en directions:', err);
          return res.status(500).json({ error: 'Error al insertar en directions.' });
        }

        res.status(201).json({ message: 'Dirección añadida con éxito', directionsId: result.insertId });
      });
    });
  });
});

//Ruta para obtener todas las direcciones de un user
app.get('/api/directions/:user_id', (req, res) => {
  const { user_id } = req.params;

  if (!user_id) {
    return res.status(400).json({ error: 'El user_id es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Consulta para obtener todas las direcciones del usuario junto con los detalles de address
    const query = `
      SELECT d.id AS direction_id, a.id AS address_id, a.address_type, a.street_number, a.address_1, a.address_2, a.postal_code, a.city, a.state, a.country
      FROM directions d
      JOIN address a ON d.address_id = a.id
      WHERE d.user_id = ?
    `;

    connection.query(query, [user_id], (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener las direcciones:', err);
        return res.status(500).json({ error: 'Error al obtener las direcciones.' });
      }

      if (results.length === 0) {
        return res.status(200).json({ message: 'No se encontraron direcciones para este usuario.', notFound: true });
      }

      res.status(200).json({ directions: results });
    });
  });
});

//Actualziar address 
app.put('/api/address/:id', (req, res) => {
  const { id } = req.params; // ID de la address a actualizar
  const { address_type, street_number, address_1, address_2, postal_code, city, state, country } = req.body;

  // Verificar que los campos requeridos estén presentes
  if (!address_type || !address_1 || !postal_code || !city || !state || !country) {
    return res.status(400).json({ error: 'Algunos campos requeridos faltan.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Actualizar la dirección en la tabla address
    const addressQuery = `
      UPDATE address 
      SET address_type = ?, street_number = ?, address_1 = ?, address_2 = ?, postal_code = ?, city = ?, state = ?, country = ?
      WHERE id = ?`;
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country, id];

    connection.query(addressQuery, addressValues, (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar la dirección:', err);
        return res.status(500).json({ error: 'Error al actualizar la dirección.' });
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ error: 'Dirección no encontrada.' });
      }

      res.status(200).json({ message: 'Dirección actualizada con éxito' });
    });
  });
});

//Borrar address por su id
app.delete('/api/address/:id', (req, res) => {
  const { id } = req.params; // ID de la dirección a eliminar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Eliminar la dirección en la tabla address
    const deleteQuery = 'DELETE FROM address WHERE id = ?';
    const deleteValues = [id];

    connection.query(deleteQuery, deleteValues, (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la dirección:', err);
        return res.status(500).json({ error: 'Error al eliminar la dirección.' });
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ error: 'Dirección no encontrada.' });
      }

      res.status(200).json({ message: 'Dirección eliminada con éxito' });
    });
  });
});

//Crear reserva
app.post('/api/bookings', (req, res) => {
  const {
    user_id,
    address_type,
    street_number,
    address_1,
    address_2,
    postal_code,
    city,
    state,
    country,
    service_id,
    booking_start_datetime,
    booking_end_datetime,
    recurrent_pattern_id,
    promotion_id,
    service_duration,
    final_price,
    commission,
    description // Nueva propiedad para la descripción
  } = req.body;

  // Verificación de campos requeridos para el usuario
  if (!user_id) {
    return res.status(400).json({ error: 'El campo user_id es requerido.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  // Variable para almacenar el address_id
  let addressId = null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Paso 1: Verificar si se necesita insertar la dirección
    if (address_type && address_1 && postal_code && city && state && country) {
      // Insertar la dirección en `address`
      const addressQuery = 'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
      const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country];

      connection.query(addressQuery, addressValues, (err, result) => {
        if (err) {
          connection.release();
          console.error('Error al insertar la dirección:', err);
          return res.status(500).json({ error: 'Error al insertar la dirección.' });
        }

        addressId = result.insertId; // ID de la dirección recién insertada

        // Paso 2: Insertar en la tabla `booking`
        createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res);
      });
    } else {
      // Si no se necesita una dirección, se usa NULL para address_id
      createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res);
    }
  });
});

// Función para crear la reserva 
function createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res) {
  const bookingQuery = `
    INSERT INTO booking (user_id, service_id, address_id, payment_method_id, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, is_paid, booking_status, description, order_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, NOW())
  `;
  const bookingValues = [
    user_id,
    service_id,
    addressId, // Esto puede ser null
    null, // payment_method_id se establece en NULL
    booking_start_datetime || null,
    booking_end_datetime || null,
    recurrent_pattern_id || null,
    promotion_id || null,
    service_duration || null,
    final_price || null,
    commission || null,
    false, // is_paid
    description || null // Se establece la descripción, puede ser null
  ];

  connection.query(bookingQuery, bookingValues, (err, result) => {
    if (err) {
      connection.release();
      console.error('Error al insertar la reserva:', err);
      return res.status(500).json({ error: 'Error al insertar la reserva.' });
    }

    const newBookingId = result.insertId;
    const selectQuery = 'SELECT * FROM booking WHERE id = ?';
    connection.query(selectQuery, [newBookingId], (selectErr, bookingData) => {
      connection.release();
      if (selectErr) {
        console.error('Error al obtener la reserva creada:', selectErr);
        return res.status(500).json({ error: 'Error al obtener la reserva creada.' });
      }
      if (bookingData.length === 0) {
        return res.status(500).json({ error: 'No se encontró la reserva creada.' });
      }

      res.status(201).json({ message: 'Reserva creada con éxito', booking: bookingData[0] });
    });
  });
}

// Obtener detalles de una reserva
app.get('/api/bookings/:id', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      SELECT
        b.id,
        b.user_id,
        b.service_id,
        b.address_id,
        b.payment_method_id,
        b.booking_start_datetime,
        b.booking_end_datetime,
        b.recurrent_pattern_id,
        b.promotion_id,
        b.service_duration,
        b.final_price,
        b.commission,
        b.is_paid,
        b.booking_status,
        b.order_datetime,
        b.description,
        pr.price,
        pr.price_type,
        pm.provider,
        pm.card_number,
        pm.expiry_date,
        pm.is_safed,
        pm.is_default,
        a.address_type,
        a.street_number,
        a.address_1,
        a.address_2,
        a.postal_code,
        a.city,
        a.state,
        a.country
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price pr ON s.price_id = pr.id
      LEFT JOIN payment_method pm ON b.payment_method_id = pm.id
      LEFT JOIN address a ON b.address_id = a.id
      WHERE b.id = ?
    `;

    connection.query(query, [id], (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al obtener la reserva:', err);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (result.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      res.status(200).json(result[0]);
    });
  });
});

// Actualizar una reserva
app.put('/api/bookings/:id', (req, res) => {
  const { id } = req.params;
  const { booking_start_datetime, booking_end_datetime, service_duration, final_price, commission, description } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      UPDATE booking SET booking_start_datetime = ?, booking_end_datetime = ?, service_duration = ?, final_price = ?, commission = ?, description = ?
      WHERE id = ?
    `;

    const values = [booking_start_datetime, booking_end_datetime, service_duration, final_price, commission, description, id];

    connection.query(query, values, (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al actualizar la reserva:', err);
        return res.status(500).json({ error: 'Error al actualizar la reserva.' });
      }

      res.status(200).json({ message: 'Reserva actualizada con éxito' });
    });
  });
});

// Actualizar el estado de una reserva
app.patch('/api/bookings/:id/status', (req, res) => {
  const { id } = req.params;
  const { status, is_paid } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const fields = [];
    const values = [];

    if (typeof status !== 'undefined') {
      fields.push('booking_status = ?');
      values.push(status);
    }
    if (typeof is_paid !== 'undefined') {
      fields.push('is_paid = ?');
      values.push(is_paid);
    }

    if (fields.length === 0) {
      connection.release();
      return res.status(400).json({ error: 'No fields to update.' });
    }

    const query = `UPDATE booking SET ${fields.join(', ')} WHERE id = ?`;
    values.push(id);

    connection.query(query, values, (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al actualizar el estado de la reserva:', err);
        return res.status(500).json({ error: 'Error al actualizar la reserva.' });
      }
      res.status(200).json({ message: 'Estado actualizado' });
    });
  });
});

// Actualizar el pago de una reserva
app.patch('/api/bookings/:id/is_paid', (req, res) => {
  const { id } = req.params;
  const { is_paid } = req.body;

  if (typeof is_paid === 'undefined') {
    return res.status(400).json({ error: 'is_paid es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'UPDATE booking SET is_paid = ? WHERE id = ?';

    connection.query(query, [is_paid, id], (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al actualizar el pago de la reserva:', err);
        return res.status(500).json({ error: 'Error al actualizar la reserva.' });
      }
      res.status(200).json({ message: 'Pago actualizado' });
    });
  });
});

// Pago de comisión al crear la reserva (10% o mínimo 1€)
app.post('/api/bookings/:id/deposit', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'SELECT final_price, commission FROM booking WHERE id = ?';
    connection.query(query, [id], async (err, results) => {
      connection.release();
      if (err) {
        console.error('Error al obtener la reserva:', err);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      const finalPrice = parseFloat(results[0].final_price || 0);
      const commission = parseFloat(results[0].commission || 0);

      try {
        const intent = await stripe.paymentIntents.create({
          amount: Math.round(commission * 100),
          currency: 'eur',
          metadata: { booking_id: id, type: 'deposit' }
        });
        res.status(200).json({ clientSecret: intent.client_secret });
      } catch (stripeErr) {
        console.error('Error al crear el pago:', stripeErr);
        res.status(500).json({ error: 'Error al procesar el pago.' });
      }
    });
  });
});

// Pago final de una reserva completada
app.post('/api/bookings/:id/final-payment', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'SELECT final_price, commission FROM booking WHERE id = ?';
    connection.query(query, [id], async (err, results) => {
      connection.release();
      if (err) {
        console.error('Error al obtener la reserva:', err);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      const finalPrice = parseFloat(results[0].final_price || 0);
      const commission = parseFloat(results[0].commission || 0);
      const amountToPay = Number((finalPrice - commission).toFixed(2));
      if (amountToPay <= 0) {
        return res.status(400).json({ error: 'El importe final es cero o negativo.' });
      }

      try {
        const intent = await stripe.paymentIntents.create({
          amount: Math.round(amountToPay * 100),
          currency: 'eur',
          metadata: { booking_id: id, type: 'final' }
        });
        res.status(200).json({ clientSecret: intent.client_secret });
      } catch (stripeErr) {
        console.error('Error al crear el pago final:', stripeErr);
        res.status(500).json({ error: 'Error al procesar el pago final.' });
      }
    });
  });
});

// Crear método de cobro y cuenta Stripe Connect
app.post('/api/user/:id/collection-method', authenticateToken, (req, res) => {
  const { id } = req.params;
  const {
    full_name,
    date_of_birth,
    nif,
    iban,
    address_type,
    street_number,
    address_1,
    address_2,
    postal_code,
    city,
    state,
    country,
    phone,
    fileTokenAnverso,
    fileTokenReverso
  } = req.body;

  if (
    !full_name ||
    !date_of_birth ||
    !nif ||
    !iban ||
    !address_type ||
    !address_1 ||
    !postal_code ||
    !city ||
    !state ||
    !country ||
    !phone ||
    !fileTokenAnverso ||
    !fileTokenReverso
  ) {
    return res.status(400).json({ error: 'Campos requeridos faltantes' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const userQuery = 'SELECT email, first_name, surname FROM user_account WHERE id = ?';
    connection.query(userQuery, [id], async (userErr, userRes) => {
      if (userErr) {
        connection.release();
        console.error('Error al obtener el usuario:', userErr);
        return res.status(500).json({ error: 'Error al obtener el usuario.' });
      }

      if (userRes.length === 0) {
        connection.release();
        return res.status(404).json({ message: 'Usuario no encontrado' });
      }

      const user = userRes[0];
      const [year, month, day] = date_of_birth.split('-').map(Number);

      try {
        const account = await stripe.accounts.create({
          type: 'custom',
          country: country.toUpperCase(),
          email: user.email,
          business_type: 'individual',
          individual: {
            first_name: user.first_name,
            last_name: user.surname,
            id_number: nif,
            dob: { day, month, year },
            address: {
              line1: address_1,
              line2: address_2 || undefined,
              postal_code,
              city,
              state,
              country: country.toUpperCase()
            },
            email: user.email, 
            phone,
            verification: {
              document: {
                front: fileTokenAnverso,
                back: fileTokenReverso
              }
            }
          },
          capabilities: {
            card_payments: { requested: true },
            transfers: { requested: true }
          },
          business_profile: {
            mcc: '7299',                      // Servicios varios (cambia si procede)
            product_description: 'Servicios profesionales'
          },
          tos_acceptance: {
            date: Math.floor(Date.now() / 1000), // fecha en segundos
            ip: req.ip                           // IP real del usuario que acepta
          }
        });

        const bank = await stripe.accounts.createExternalAccount(account.id, {
          external_account: {
            object: 'bank_account',
            country: country.toUpperCase(),
            currency: 'eur',
            account_holder_name: full_name,
            account_number: iban
          }
        });

        const addressQuery =
          'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
        const streetNumberValue = street_number || null;
        const address2Value = address_2 || null;
        const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country];
        connection.query(addressQuery, addressValues, (addrErr, addrRes) => {
          if (addrErr) {
            connection.release();
            console.error('Error al guardar la dirección:', addrErr);
            return res.status(500).json({ error: 'Error al guardar la dirección.' });
          }

          const insertMethodQuery =
            'INSERT INTO collection_method (user_id, type, external_account_id, last4, brand, address_id, full_name) VALUES (?, ?, ?, ?, ?, ?, ?)';
          const last4 = iban.slice(-4);
          connection.query(
            insertMethodQuery,
            [id, 'iban', bank.id, last4, null, addrRes.insertId, full_name],
            (cmErr) => {
              if (cmErr) {
                connection.release();
                console.error('Error al guardar el método de cobro:', cmErr);
                return res.status(500).json({ error: 'Error al guardar el método de cobro.' });
              }

              const updateQuery =
                'UPDATE user_account SET date_of_birth = ?, nif = ?, phone = ?, stripe_account_id = ?, is_professional = 1 WHERE id = ?';
              connection.query(updateQuery, [date_of_birth, nif, phone, account.id, id], (updErr) => {
                connection.release();
                if (updErr) {
                  console.error('Error al actualizar el usuario:', updErr);
                  return res.status(500).json({ error: 'Error al guardar la cuenta.' });
                }
                res.status(201).json({ message: 'Método de cobro creado', stripe_account_id: account.id });
              });
            }
          );
        });
      } catch (stripeErr) {
        connection.release();
        console.error('Error al crear la cuenta de Stripe:', stripeErr);
        res.status(500).json({ error: 'Error al crear la cuenta de cobro.' });
      }
    });
  });
});

// Transferir el pago final al profesional con Stripe Connect (NO ACTIVO!)
app.post('/api/bookings/:id/transfer', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `SELECT b.final_price, b.commission, s.user_id, u.stripe_account_id
                   FROM booking b
                   JOIN service s ON b.service_id = s.id
                   JOIN user_account u ON s.user_id = u.id
                   WHERE b.id = ?`;

    connection.query(query, [id], async (qErr, results) => {
      connection.release();
      if (qErr) {
        console.error('Error al obtener la reserva:', qErr);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      const { final_price, commission, stripe_account_id } = results[0];

      if (!stripe_account_id) {
        return res.status(400).json({ error: 'El profesional no tiene cuenta Stripe.' });
      }

      const finalPrice = parseFloat(results[0].final_price || 0);
      const commissionAmount = parseFloat(results[0].commission || 0);
      const amount = Number((finalPrice - commissionAmount).toFixed(2));
      if (amount <= 0) {
        return res.status(400).json({ error: 'El importe a transferir es cero o negativo.' });
      }

      try {
        await stripe.transfers.create({
          amount: Math.round(amount * 100),
          currency: 'eur',
          destination: stripe_account_id,
          metadata: { booking_id: id }
        });
        res.status(200).json({ message: 'Transferencia realizada con éxito' });
      } catch (stripeErr) {
        console.error('Error al realizar la transferencia:', stripeErr);
        res.status(500).json({ error: 'Error al realizar la transferencia.' });
      }
    });
  });
});

// Pago final y transferencia automática al profesional (destination charge!) ------
app.post('/api/bookings/:id/final-payment-transfer', authenticateToken, async (req, res) => {
  const { id } = req.params;
  const { payment_method_id } = req.body;
  if (!payment_method_id) {
    return res.status(400).json({ error: 'payment_method_id es requerido.' });
  }

  const baseKey = req.headers['x-idempotency-key'] || crypto.randomUUID();

  pool.getConnection(async (err, connection) => {
    if (err) return res.status(500).json({ error: 'Error al obtener la conexión.' });
    const conn = connection.promise();

    try {
      await conn.beginTransaction();

      const [[booking]] = await conn.query(`
        SELECT b.final_price,
               b.commission,
               b.is_paid,
               u.stripe_account_id
        FROM booking b
        JOIN service s  ON b.service_id = s.id
        JOIN user_account u ON s.user_id = u.id
        WHERE b.id = ? FOR UPDATE
      `, [id]);

      if (!booking) throw new NotFound('Reserva no encontrada.');
      if (booking.is_paid) throw new Conflict('Esta reserva ya está pagada.');
      if (!booking.stripe_account_id) throw new BadRequest('El profesional no tiene cuenta Stripe.');

      const finalPrice       = parseFloat(booking.final_price  || 0);
      const commissionAmount = parseFloat(booking.commission   || 0);
      const amountToCharge   = Number((finalPrice - commissionAmount).toFixed(2));
      if (amountToCharge <= 0) throw new BadRequest('Importe a cobrar inválido.');

      const intent = await stripe.paymentIntents.create({
        amount: Math.round(amountToCharge * 100),      // cents
        currency: 'eur',
        payment_method: payment_method_id,
        confirm: true,
        automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
        transfer_data: { destination: booking.stripe_account_id },
        metadata: { booking_id: id, type: 'final' }
      }, { idempotencyKey: `${baseKey}:pi` });

      if (intent.status !== 'succeeded') throw new Error('Pago no completado.');

      await conn.query('UPDATE booking SET is_paid = 1 WHERE id = ?', [id]);
      await conn.commit();

      res.status(200).json({ message: 'Pago enviado al profesional', paymentIntentId: intent.id });
    } catch (e) {
      await conn.rollback();
      console.error(e);
      res.status(e.statusCode || 500).json({ error: e.message });
    } finally {
      connection.release();
    }
  });
});

// Generar y descargar factura en PDF de una reserva pagada (2 facturas)
app.get('/api/bookings/:id/invoice', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error getting connection:', err);
      return res.status(500).json({ error: 'Connection error.' });
    }

    const query = `
      SELECT
        b.id AS booking_id,
        b.final_price,
        b.commission,
        b.is_paid,
        b.booking_start_datetime,
        b.booking_end_datetime,
        b.description AS booking_description,
        s.service_title,
        s.description AS service_description,
        cu.email AS customer_email,
        cu.phone AS customer_phone,
        cu.first_name AS customer_first_name,
        cu.surname AS customer_surname,
        sp.id AS provider_id,
        sp.email AS provider_email,
        sp.phone AS provider_phone,
        sp.first_name AS provider_first_name,
        sp.surname AS provider_surname,
        sp.nif AS provider_nif,
        a.address_1 AS provider_address_1,
        a.address_2 AS provider_address_2,
        a.street_number AS provider_street_number,
        a.postal_code AS provider_postal_code,
        a.city AS provider_city,
        a.state AS provider_state,
        a.country AS provider_country
      FROM booking b
      JOIN user_account cu ON b.user_id = cu.id
      JOIN service s ON b.service_id = s.id
      JOIN user_account sp ON s.user_id = sp.id
      LEFT JOIN (
        SELECT cm1.* FROM collection_method cm1
        JOIN (
          SELECT user_id, MAX(id) AS max_id
          FROM collection_method
          GROUP BY user_id
        ) cm2 ON cm1.user_id = cm2.user_id AND cm1.id = cm2.max_id
      ) cm ON cm.user_id = sp.id
      LEFT JOIN address a ON a.id = cm.address_id
      WHERE b.id = ?
      LIMIT 1;
    `;

    connection.query(query, [id], (err, results) => {
      connection.release();
      if (err) {
        console.error('Error fetching booking:', err);
        return res.status(500).json({ error: 'Error fetching booking.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Booking not found.' });
      }

      const data = results[0];
      const doc = new PDFDocument({ margins: { top: 64, left: 64, right: 64, bottom: 64 } });

      // Resource paths
      const assetsPath = path.join(__dirname, 'assets');
      doc.registerFont('Inter', path.join(assetsPath, 'fonts', 'Inter-Regular.ttf'));
      doc.registerFont('Inter-Bold', path.join(assetsPath, 'fonts', 'Inter-Bold.ttf'));

      // Capture PDF data in memory
      const buffers = [];
      doc.on('data', buffers.push.bind(buffers));
      doc.on('end', () => {
        const pdfData = Buffer.concat(buffers);
        res.set({
          'Content-Type': 'application/pdf',
          'Content-Disposition': `attachment; filename=invoice_${id}.pdf`,
          'Content-Length': pdfData.length
        });
        res.send(pdfData);
      });

      // Helpers
      const formatDate = (value) => {
        try {
          const d = new Date(value);
          const dd = String(d.getDate()).padStart(2, '0');
          const mm = String(d.getMonth() + 1).padStart(2, '0');
          const yyyy = d.getFullYear();
          return `${dd}/${mm}/${yyyy}`;
        } catch (_) {
          return '';
        }
      };
      const toCurrency = (amount) => `€${Number(amount || 0).toFixed(2)}`;

      // Decide invoice type
      const typeParam = String(req.query.type || '').toLowerCase();
      const invoiceType = (typeParam === 'deposit' || typeParam === 'final')
        ? typeParam
        : (data.is_paid ? 'final' : 'deposit');

      // VAT configuration (only used for provider invoice)
      const vatRateParam = req.query.vat_rate;
      let vatRateProvider = 21; // default 21%
      if (vatRateParam !== undefined) {
        const parsed = parseInt(vatRateParam, 10);
        if (!Number.isNaN(parsed) && parsed >= 0 && parsed <= 21) vatRateProvider = parsed;
      }
      const isExempt = String(req.query.exempt || '').toLowerCase() === 'true';
      const isReverseCharge = String(req.query.reverse_charge || '').toLowerCase() === 'true';

      // Common header
      let logoX, logoY, logoWidth, logoHeight;
      logoWidth = 60;
      logoX = doc.page.width - doc.page.margins.right - logoWidth;
      logoY = doc.page.margins.top;
      logoHeight = 0; // por defecto

      try {
        const logoPath = path.join(assetsPath, 'wisdom.png');

        // Si tu versión de PDFKit soporta openImage, úsalo para conocer el alto real
        if (typeof doc.openImage === 'function') {
          const img = doc.openImage(logoPath);
          logoHeight = Math.round(logoWidth * (img.height / img.width));
        } else {
          // fallback razonable si no hay openImage (supón cuadrado)
          logoHeight = logoWidth;
        }

        doc.image(logoPath, logoX, logoY, { width: logoWidth });
      } catch (e) {
        console.warn('Logo not found:', e);
        // mantenemos logoHeight como esté (0 o fallback) y seguimos
      }

      // Título debajo del logo (o del hueco del logo si no se pudo cargar)
      const titleY = logoY + (logoHeight || logoWidth) + 8;
      doc.font('Inter-Bold').fontSize(20);
      doc.font('Inter-Bold').fontSize(20);
      const innerWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
      doc.text('INVOICE', doc.page.margins.left, titleY, {
        width: innerWidth,
        align: 'center'
      });

      // Vuelve el cursor al margen izquierdo para el resto de contenido
      doc.x = doc.page.margins.left;
      doc.moveDown(1.2);

      // (opcional, evita que un error de PDF mate el dyno)
      doc.on('error', (err) => {
        console.error('PDF error:', err);
        if (!res.headersSent) res.status(500).send('PDF generation error');
      });


      // Metadata
      const now = new Date();
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, '0');
      const seriesNumber = invoiceType === 'deposit'
        ? `WISDOM-${yyyy}-${mm}-${data.booking_id}`
        : `PRO-${data.provider_id}-${yyyy}-${mm}-${data.booking_id}`;

      doc.font('Inter-Bold').fontSize(11).text('Series & No.');
      doc.font('Inter').fontSize(11).text(seriesNumber);
      doc.moveDown(0.4);
      doc.font('Inter-Bold').text('Issue date');
      doc.font('Inter').text(formatDate(now));
      doc.moveDown(0.4);
      doc.font('Inter-Bold').text(invoiceType === 'deposit' ? 'Date of the transaction' : 'Date of the transaction (if different)');
      doc.font('Inter').text(invoiceType === 'deposit' ? '—' : formatDate(data.booking_end_datetime || data.booking_start_datetime));

      doc.moveDown();
      doc.moveDown();

      // ISSUER
      doc.font('Inter-Bold').fontSize(12).text('ISSUER');
      doc.moveDown(0.3);
      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(11);
        doc.text('Name or company name: WISDOM, S.L.');
        doc.text('Tax ID: 39414159W');
        doc.text('Address: Font dels Reis, 60, 008304, Mataró, Barcelona, Spain');
      } else {
        const providerFullName = `${data.provider_first_name || ''} ${data.provider_surname || ''}`.trim();
        const addrParts = [
          [data.provider_address_1, data.provider_street_number].filter(Boolean).join(' '),
          [data.provider_postal_code, data.provider_city].filter(Boolean).join(', '),
          [data.provider_state, data.provider_country].filter(Boolean).join(', ')
        ].filter(Boolean).join(', ');

        doc.font('Inter').fontSize(11);
        doc.text(`Name or company name: ${providerFullName || '—'}`);
        doc.text(`Tax ID: ${data.provider_nif || '__________'}`);
        doc.text(`Address: ${addrParts || '—'}`);
      }

      doc.moveDown();

      // RECIPIENT
      doc.font('Inter-Bold').fontSize(12).text('RECIPIENT');
      doc.moveDown(0.3);
      const customerFullName = `${data.customer_first_name || ''} ${data.customer_surname || ''}`.trim();
      doc.font('Inter').fontSize(11);
      doc.text(`Full name: ${customerFullName || '—'}`);
      doc.text('Tax ID: —');
      doc.text('Address: —');

      doc.moveDown();

      // DESCRIPTION
      doc.font('Inter-Bold').fontSize(12).text('DESCRIPTION');
      doc.moveDown(0.3);
      const serviceTitleQuoted = data.service_title ? `"${data.service_title}"` : '""';
      const bookingDescQuoted = data.booking_description ? ` with description "${data.booking_description}"` : '';
      const serviceSummary = `${serviceTitleQuoted}${bookingDescQuoted}`.trim();
      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(11).text(
          `Item: Service fee for intermediation in booking ${data.booking_id} of the service ${serviceSummary}, with scheduled date of provision ${formatDate(data.booking_start_datetime)}.`
        );
      } else {
        doc.font('Inter').fontSize(11).text(
          `Item: Provision of the service ${serviceSummary} carried out on ${formatDate(data.booking_end_datetime || data.booking_start_datetime)}. Booking reference in Wisdom: ${data.booking_id}`
        );
      }

      doc.moveDown();

      // TAX DETAILS
      doc.font('Inter-Bold').fontSize(12).text('TAX DETAILS');
      doc.moveDown(0.3);

      let taxableBase = 0;
      let vatRate = 21;
      let vatAmount = 0;
      let invoiceTotal = 0;

      if (invoiceType === 'deposit') {
        invoiceTotal = Number(data.commission || 0);
        vatRate = 21;
        taxableBase = Number((invoiceTotal / (1 + vatRate / 100)).toFixed(2));
        vatAmount = Number((invoiceTotal - taxableBase).toFixed(2));
      } else {
        invoiceTotal = Number((Number(data.final_price || 0) - Number(data.commission || 0)).toFixed(2));
        vatRate = isExempt || isReverseCharge ? 0 : vatRateProvider;
        if (vatRate > 0) {
          taxableBase = Number((invoiceTotal / (1 + vatRate / 100)).toFixed(2));
          vatAmount = Number((invoiceTotal - taxableBase).toFixed(2));
        } else {
          taxableBase = invoiceTotal;
          vatAmount = 0;
        }
      }

      doc.font('Inter').fontSize(11);
      doc.text(`Taxable base: ${toCurrency(taxableBase)}`);
      doc.text(`VAT rate: ${vatRate > 0 ? `${vatRate}%` : (isExempt ? 'exempt' : (isReverseCharge ? 'reverse charge' : '0%'))}`);
      doc.text(`VAT amount: ${toCurrency(vatAmount)}`);
      doc.text(`Invoice total: ${toCurrency(invoiceTotal)}`);

      doc.moveDown();
      doc.moveDown();

      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(10).fillColor('#6B7280').text(
          `This invoice refers exclusively to Wisdom’s intermediation service fee. The professional service will be invoiced by the service provider upon completion.`,
          { align: 'left' }
        );
      } else {
        doc.font('Inter').fontSize(10).fillColor('#6B7280').text(
          `Issued by a third party on behalf of and in the name of the issuer (Wisdom), pursuant to Article 5 of the Spanish Invoicing Regulations.`,
          { align: 'left' }
        );
        if (isExempt) {
          doc.moveDown(0.2);
          doc.text(`exempt under Art. 20 LIVA`, { align: 'left' });
        }
        if (isReverseCharge) {
          doc.moveDown(0.2);
          doc.text(`reverse charge`, { align: 'left' });
        }
      }

      // Footer divider
      doc.moveDown(2);
      doc.fillColor('#000000');

      doc.end();
    });
  });
});

// Borrar una reserva por su id
app.delete('/api/delete_booking/:id', (req, res) => {
  const { id } = req.params; // ID de la reserva a eliminar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const deleteQuery = 'DELETE FROM booking WHERE id = ?';

    connection.query(deleteQuery, [id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la reserva:', err);
        return res.status(500).json({ error: 'Error al eliminar la reserva.' });
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Reserva eliminada con éxito' });
      } else {
        res.status(404).json({ message: 'Reserva no encontrada' });
      }
    });
  });
});

//Ruta para obtener las sugerencias de busqueda de servicios
app.get('/api/suggestions', (req, res) => {
  const { query } = req.query;

  // Validar que se reciba el término de búsqueda
  if (!query || typeof query !== 'string' || query.trim() === '') {
    return res.status(400).json({ error: 'La consulta de búsqueda es requerida.' });
  }

  // Conectar a la base de datos
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Definir el patrón de búsqueda
    const searchPattern = `%${query}%`;

    // Consulta para obtener sugerencias de búsqueda, eliminando duplicados
    const searchQuery = `
      SELECT s.service_title, ct.service_category_name, f.service_family, t.tag 
      FROM service s 
      LEFT JOIN service_category c ON s.service_category_id = c.id 
      LEFT JOIN service_family f ON c.service_family_id = f.id 
      LEFT JOIN service_category_type ct ON c.service_category_type_id = ct.id 
      LEFT JOIN service_tags t ON s.id = t.service_id 
      WHERE 
        s.service_title LIKE ? 
        OR ct.service_category_name LIKE ? 
        OR f.service_family LIKE ? 
        OR t.tag LIKE ?
      LIMIT 8
    `;

    // Ejecutar la consulta
    connection.query(searchQuery, 
      [searchPattern, searchPattern, searchPattern, searchPattern], 
      (err, results) => {
        connection.release(); // Liberar la conexión después de usarla

        if (err) {
          console.error('Error al obtener las sugerencias:', err);
          return res.status(500).json({ error: 'Error al obtener las sugerencias.' });
        }

        // Crear un array para almacenar las sugerencias únicas
        const suggestions = [];
        const uniqueKeys = new Set(); // Usamos un Set para asegurarnos de que no haya duplicados

        results.forEach(result => {
          // Agregar un solo valor por cada tipo de sugerencia si aún no ha sido agregado
          // y asegurarse de que contenga la palabra de búsqueda
          if (result.service_title && !uniqueKeys.has(result.service_title) && result.service_title.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_title: result.service_title });
            uniqueKeys.add(result.service_title);
          }
          if (result.service_category_name && !uniqueKeys.has(result.service_category_name) && result.service_category_name.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_category_name: result.service_category_name });
            uniqueKeys.add(result.service_category_name);
          }
          if (result.service_family && !uniqueKeys.has(result.service_family) && result.service_family.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_family: result.service_family });
            uniqueKeys.add(result.service_family);
          }
          if (result.tag && !uniqueKeys.has(result.tag) && result.tag.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ tag: result.tag });
            uniqueKeys.add(result.tag);
          }
        });

        // Verificar si se encontraron resultados
        if (suggestions.length === 0) {
          return res.status(200).json({ message: 'No se encontraron sugerencias.', notFound: true });
        }

        // Devolver las sugerencias encontradas
        res.status(200).json({ suggestions }); 
      }
    );
  });
});

//Ruta para obtener todos los servicios de una busqueda 
app.get('/api/services', (req, res) => {
  const { query } = req.query; // Obtener la consulta de búsqueda de los parámetros de la solicitud

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Definir el patrón de búsqueda
    const searchPattern = `%${query}%`;

    // Consulta para obtener la información de todos los servicios, sus tags y las imágenes
    const queryServices = ` 
      SELECT 
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        
        -- Campos adicionales
        category_type.service_category_name,
        family.service_family,
        
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
        FROM service_tags 
        WHERE service_tags.service_id = service.id) AS tags,
        
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
        FROM service_image si 
        WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.service_title LIKE ?
        OR category_type.service_category_name LIKE ?
        OR family.service_family LIKE ?
        OR service.id IN (SELECT service_id FROM service_tags WHERE tag LIKE ?)
        OR service.description LIKE ?
      ORDER BY 
        CASE 
          WHEN service.service_title LIKE ? THEN 1 -- Más importante
          WHEN category_type.service_category_name LIKE ? THEN 1 -- Más importante
          WHEN family.service_family LIKE ? THEN 1 -- Más importante
          WHEN service.id IN (SELECT service_id FROM service_tags WHERE tag LIKE ?) THEN 1 -- Más importante
          WHEN service.description LIKE ? THEN 2 -- Menos importante
          ELSE 3
        END;`;

    connection.query(queryServices, [searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern], (err, servicesData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de los servicios:', err);
        res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
        return;
      }

      if (servicesData.length > 0) {
        res.status(200).json(servicesData); // Devolver la lista de servicios con tags e imágenes
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron servicios que coincidan con la búsqueda.' });
      }
    });
  });
});

//Ruta para obtener la información de un servicio por su id
app.get('/api/services/:id', (req, res) => {
  const { id } = req.params; // ID del servicio

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      SELECT
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.service_created_datetime,
        price.price,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        category_type.service_category_name,
        family.service_family,
        (SELECT JSON_ARRAYAGG(tag)
        FROM service_tags
        WHERE service_tags.service_id = service.id) AS tags,
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'order', si.order))
        FROM service_image si
        WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.id = ?;`;

    connection.query(query, [id], (err, serviceData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del servicio:', err);
        res.status(500).json({ error: 'Error al obtener la información del servicio.' });
        return;
      }

      if (serviceData.length > 0) {
        res.status(200).json(serviceData[0]); // Devolver la información del servicio
      } else {
        res.status(404).json({ notFound: true, message: 'Servicio no encontrado.' });
      }
    });
  });
}); 




app.post('/api/services/:id/reviews', (req, res) => {
  const { id } = req.params;
  const { rating, comment } = req.body;
  const userId = req.user.id;

  if (!rating) {
    return res.status(400).json({ error: 'rating es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      INSERT INTO review (user_id, service_id, rating, comment, review_datetime)
      VALUES (?, ?, ?, ?, NOW());
    `;
    connection.query(query, [userId, id, rating, comment], (err, result) => {
      connection.release();

      if (err) {
        console.error('Error al añadir la review:', err);
        return res.status(500).json({ error: 'Error al añadir la review.' });
      }

      res.status(201).json({ message: 'Review añadida con éxito', reviewId: result.insertId });
    });
  });
});

app.post('/api/upload-dni', (req, res) => {
  uploadDni.single('file')(req, res, async (err) => {
    if (err) {
      if (err.message === 'INVALID_FILE_TYPE') {
        return res.status(400).json({ error: 'Invalid file type' });
      }
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'File too large' });
      }
      return res.status(400).json({ error: err.message });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'File is required' });
    }

    const filePath = req.file.path;
    try {
      const stripeFile = await stripe.files.create({
        purpose: 'identity_document',
        file: {
          data: fs.createReadStream(filePath),
          name: req.file.originalname,
          type: req.file.mimetype,
        },
      });
      await fs.promises.unlink(filePath);
      return res.status(201).json({ fileToken: stripeFile.id });
    } catch (stripeErr) {
      await fs.promises.unlink(filePath).catch(() => {});
      console.error('Stripe file upload error:', stripeErr);
      return res.status(500).json({ error: 'Stripe upload failed' });
    }
  });
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});



