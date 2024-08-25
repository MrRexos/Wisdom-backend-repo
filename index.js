const express = require('express');
const jwt = require('jsonwebtoken');
const mysql = require('mysql2');
const bodyParser = require('body-parser');

mysql.createPool({
    user:process.env.USER,
    password:process.env.PASSWORD,
    database:process.env.DATABASE,
    host:process.env.HOST,
    connectionLimit:10
    });

const app = express();
const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));



app.get('/',function(req,res){
    res.send({
        message:'Default route'
    });
});

app.post('/signup',function(req,res){
    res.send({
        message:'Signup method here!'
    });
}); 

app.listen(PORT,()=>{
    console.log(`Listening on PORT: ${PORT}`)
});