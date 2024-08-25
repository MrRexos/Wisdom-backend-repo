const express = require('express');
const jwt = require('jsonwebtoken');
const mysql = require('mysql2');
const bodyParser = require('body-parser');

const myPool =   mysql.createPool({
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

app.post('/api/users',function(req,res){
    //res.send({
    //    message:'Signup method here!'
    //});

    var email = req.body.email;
    var password = req.body.password;

    myPool.getConnection(function(err, poolConnection){
        if(err){
            res.send({message:"Error making signup connection"});
            poolConnection.release();
        } else {
            myPool.query('INSERT INTO user(email,password,isActive)VALUES(?,?,1)', [email, password], function (err, insertResults, insertFields){
                if(err){
                    console.log(err + ' when inserting data into db');
                }else{
                    res.send({
                        message:'User inserted!'
                    });
                }  
            });
        }
    })

}); 

app.listen(PORT,()=>{
    console.log(`Listening on PORT: ${PORT}`)
});