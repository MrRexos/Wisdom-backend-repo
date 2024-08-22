const express = require('express');
const app = express();
const port = process.env.PORT || 3000;
const db = require('./db'); // Asegúrate de que este archivo se llame db.js

app.use(express.json());

// Endpoint para obtener datos de la base de datos
app.get('/api/data', (req, res) => {
  const query = 'SELECT * FROM your_table';
  db.query(query, (err, results) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(results);
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
}); 