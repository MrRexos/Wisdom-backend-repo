const express = require('express');
const jwt = require('jsonwebtoken');
const mysql2 = require('mysql2');
const bcrypt = require('bcrypt');
const bodyParser = require('body-parser');
const app = express();
const PORT = process.env.PORT || 3000;

// Crear el pool de conexiones a la base de datos
const myPool = mysql2.createPool({
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE,
    host: process.env.HOST,
    connectionLimit: 10
});

// Middleware para manejar JSON y URL encoded
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

// Ruta de registro
app.post('/signup', (req, res) => {
    const email = req.body.email;
    const password = req.body.password;

    myPool.getConnection((err, poolConnection) => {
        if (err) {
            return res.status(500).send({ message: "Error connecting to the database" });
        }

        poolConnection.query('SELECT COUNT(*) AS EmailCount FROM user WHERE email = ?', [email], (err, results) => {
            if (err) {
                poolConnection.release();
                console.error('Error selecting email count:', err);
                return res.status(500).send({ message: "Error checking email" });
            }

            const emailCount = results[0].EmailCount;

            if (emailCount > 0) {
                poolConnection.release();
                return res.status(400).send({ message: 'This email address is already associated with an account' });
            }

            const saltRounds = 10;
            bcrypt.hash(password, saltRounds, (err, hashedPassword) => {
                if (err) {
                    poolConnection.release();
                    console.error('Error hashing password:', err);
                    return res.status(500).send({ message: "Error hashing password" });
                }

                poolConnection.query('INSERT INTO user (email, password, isActive) VALUES (?, ?, 1)', [email, hashedPassword], (err, insertResults) => {
                    poolConnection.release();
                    if (err) {
                        console.error('Error inserting data into database:', err);
                        return res.status(500).send({ message: "Error inserting user" });
                    }

                    res.status(201).send({ message: 'User registered successfully!' });
                });
            });
        });
    });
});

app.listen(PORT, () => {
    console.log(`Server is listening on PORT: ${PORT}`);
});



app.get('/',function(req,res){
    res.send({
        message:'Default route'
    });
});

app.listen(PORT,()=>{
    console.log(`Listening on PORT: ${PORT}`)
});

