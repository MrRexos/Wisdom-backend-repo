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

const Stripe = require('stripe');
const stripe = new Stripe('tu_clave_secreta');

const app = express();
const port = process.env.PORT || 3000;

// Middleware para parsear JSON. 
app.use(bodyParser.json());
app.use(express.json());
const upload = multer({ storage: multer.memoryStorage() });

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
  const { email, username, password, first_name, surname, language, allow_notis, profile_picture } = req.body;

  try {
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    const query = 'INSERT INTO user_account (email, username, password, first_name, surname, joined_datetime, language, allow_notis, profile_picture, is_verified) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, 0)';
    const values = [email, username, hashedPassword, first_name, surname, language, allow_notis, profile_picture];

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
                            
                            <svg
                              viewBox="-5000 -15000 40000 10000"
                              width="18"
                              height="18"
                              fill="#111827"
                              aria-hidden="true"
                            >
                                                
                              <g transform="translate(0,0) scale(1,-1)">
                                <path d="M 345 17646 c -160 -39 -280 -158 -321 -316 c -22 -88 -15 -200 18 -272 c 31 -67 9607 -16794 9656 -16867 c 49 -73 136 -136 224 -162 c 57 -18 87 -21 160 -17 c 77 5 100 11 163 42 c 50 25 88 53 122 90 c 33 37 823 1364 2532 4255 c 1366 2310 2487 4201 2490 4201 c 3 0 1124 -1891 2491 -4203 c 1417 -2396 2502 -4221 2525 -4246 c 116 -127 296 -175 456 -121 c 118 40 182 95 257 222 c 228 387 9617 16799 9628 16830 c 85 242 -77 520 -331 567 c -79 15 -9438 16 -9516 1 c -78 -15 -169 -63 -224 -118 c -36 -36 -654 -1074 -2660 -4466 c -1438 -2430 -2617 -4422 -2622 -4427 c -4 -4 -1187 1986 -2628 4423 c -2768 4681 -2644 4475 -2755 4541 c -105 61 276 57 -4895 56 c -3688 -1 -4731 -3 -4770 -13 Z" />
                              </g>
                            </svg>

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
                            <svg
                              viewBox="0 0 5120 5120"
                              width="18"
                              height="18"
                              fill="#111827"
                              style="display:block;margin:auto;"
                              aria-hidden="true"
                            >
                              <path d="M 1525 4798 c -356 -40 -708 -245 -939 -549 c -139 -184 -240 -438 -266 -671 c -14 -132 -14 -1904 0 -2036 c 41 -372 241 -720 551 -956 c 184 -139 438 -240 671 -266 c 132 -14 1904 -14 2036 0 c 633 69 1153 589 1222 1222 c 14 132 14 1904 0 2036 c -69 633 -589 1153 -1222 1222 c -120 12 -1937 11 -2053 -2 Z m 2138 -365 c 212 -60 360 -147 502 -295 c 138 -142 214 -279 268 -481 c 22 -82 22 -86 22 -1097 c 0 -1011 0 -1015 -22 -1097 c -54 -202 -130 -339 -268 -481 c -144 -150 -297 -239 -507 -295 l -83 -22 l -1015 0 l -1015 0 l -83 22 c -210 56 -363 145 -507 295 c -138 142 -214 279 -268 481 c -22 82 -22 86 -22 1097 c 0 1011 0 1015 22 1097 c 54 202 130 339 268 481 c 130 135 253 213 428 271 c 148 50 146 50 1207 48 c 994 -2 995 -2 1073 -24 Z"></path>
                            
                              <path d="M 3750 1510 c -127 -38 -204 -177 -167 -301 c 52 -179 274 -238 405 -107 c 65 65 87 158 59 250 c -14 50 -69 114 -118 139 c -47 24 -133 33 -179 19 Z"></path>
                            
                              <path d="M 2436 3800 c -314 -36 -598 -185 -809 -425 c -524 -598 -371 -1519 319 -1919 c 355 -206 810 -223 1169 -44 c 262 131 446 311 580 570 c 193 372 181 825 -31 1192 c -182 316 -489 535 -854 610 c -97 19 -274 27 -374 16 Z m 370 -375 c 298 -75 548 -317 640 -620 c 26 -83 28 -103 28 -255 c 0 -152 -2 -172 -28 -255 c -94 -306 -315 -527 -621 -621 c -83 -26 -103 -28 -255 -28 c -152 0 -172 2 -255 28 c -493 150 -763 668 -605 1161 c 60 185 221 384 400 494 c 200 122 453 157 696 96 Z"></path>
                            </svg>
                          </a>
                        </td>
                        <td style="padding:0 0px;">
                          <a href="https://twitter.com/" aria-label="Twitter"
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
                            <svg viewBox="0 0 21 24" width="18" height="18" fill="#111827" style="display:block;margin:auto;">
                              <path d="M8 19c5.5 0 10-4.5 10-10 0-.2 0-.4 0-.6a7.2 7.2 0 0 0 1.8-1.9c-.7.3-1.5.5-2.3.5a4 4 0 0 0 1.7-2.2 8 8 0 0 1-2.6 1 4 4 0 0 0-6.8 3.6A11.3 11.3 0 0 1 3 5.2a4 4 0 0 0 1.3 5.3A4 4 0 0 1 2.4 10v.1a4 4 0 0 0 3.2 3.9 4 4 0 0 1-1.8.1 4 4 0 0 0 3.7 2.7A8.1 8.1 0 0 1 2 18.6a11.3 11.3 0 0 0 6 1.8"/>
                            </svg>
                          </a>
                        </td>
                      </tr>
                    </table>

                    <!-- PIE DE PÁGINA -->
                    <div style="font-size:12px;color:#6b7280;line-height:1.4;">
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
        res.send('Cuenta verificada con éxito');
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
    const query = `
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
    const query = `
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

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas y servicios asociados
    const query = `
      SELECT 
          booking.id AS booking_id,
          booking.booking_start_datetime,
          booking.booking_end_datetime,
          booking.service_duration,
          booking.final_price,
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
      WHERE booking.user_id = ?
      ORDER BY booking.booking_start_datetime DESC;

    `;

    connection.query(query, [userId], (err, bookingsData) => {
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

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas donde el servicio pertenece a un usuario específico
    const query = `
      SELECT 
        booking.id AS booking_id,
        booking.user_id AS booking_user_id,
        booking.booking_start_datetime,
        booking.booking_end_datetime,
        booking.service_duration,
        booking.final_price,
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
        service_user.username AS service_user_username,
        service_user.first_name AS service_user_first_name,
        service_user.surname AS service_user_surname,
        service_user.profile_picture AS service_user_profile_picture,
        service_user.is_professional AS service_user_is_professional,
        service_user.language AS service_user_language,
        -- Información del usuario que realizó la reserva
        booking_user.id AS booking_user_id,
        booking_user.email AS booking_user_email,
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
      WHERE service.user_id = ? -- Filtrar por el user_id dentro de la tabla service
      ORDER BY booking.booking_start_datetime DESC; -- Ordenar de más reciente a más antiguo
    `;

    connection.query(query, [userId], (err, bookingsData) => {
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

//Ruta para actualizar el profile
app.put('/api/user/:id/profile', (req, res) => {
  const { id } = req.params; // ID del usuario
  const { profile_picture, username, first_name, surname } = req.body; // Datos a actualizar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para actualizar los valores en user_account
    const query = `
      UPDATE user_account
      SET profile_picture = ?, username = ?, first_name = ?, surname = ?
      WHERE id = ?;
    `;

    connection.query(query, [profile_picture, username, first_name, surname, id], (err, result) => {
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
        createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, description, res);
      });
    } else {
      // Si no se necesita una dirección, se usa NULL para address_id
      createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, description, res);
    }
  });
});

// Función para crear la reserva 
function createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, description, res) {
  const bookingQuery = `
    INSERT INTO booking (user_id, service_id, address_id, payment_method_id, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, is_paid, booking_status, description, order_datetime) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, NOW())
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
    false, // is_paid
    description || null // Se establece la descripción, puede ser null
  ];

  connection.query(bookingQuery, bookingValues, (err, result) => {
    connection.release(); // Liberar la conexión después de usarla

    if (err) {
      console.error('Error al insertar la reserva:', err);
      return res.status(500).json({ error: 'Error al insertar la reserva.' });
    }

    res.status(201).json({ message: 'Reserva creada con éxito', bookingId: result.insertId });
  });
}

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




// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});



