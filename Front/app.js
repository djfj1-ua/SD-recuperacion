const express = require('express');
const axios = require('axios');
const app = express();
const PORT = 3000;

app.set('view engine', 'ejs');
app.use(express.static('public'));

// Ruta principal
app.get('/', async (req, res) => {
    try {
        // Llamadas a la Central y a la CTC
        const centralEstado = await axios.get('http://127.0.0.1:8000/estado');  // Cambia el puerto según tu Central
        const ctcEstado = await axios.get('http://127.0.0.1:5000/consulta');    // CTC estado

        const { taxis, clientes, mapa } = centralEstado.data;
        const estadoCTC = ctcEstado.data.estado;

        // Convertir el mapa a texto (cada celda a string)
        const mapaTexto = mapa.map(fila => fila.map(celda => String(celda)));

        res.render('index', {
            taxis: taxis,
            clientes: clientes,
            mapa: mapaTexto,
            estadoCTC: estadoCTC
        });

    } catch (error) {
        console.error('Error obteniendo datos:', error.message);
        res.send('<h1>Error conectando a EC_Central o CTC</h1>');
    }
});

app.get('/api/estado', async (req, res) => {
    try {
        const centralEstado = await axios.get('http://127.0.0.1:8000/estado');
        const ctcEstado = await axios.get('http://127.0.0.1:5000/consulta');

        const { taxis, clientes, mapa } = centralEstado.data;
        const estadoCTC = ctcEstado.data.estado;

        res.json({
            taxis: taxis,
            clientes: clientes,
            mapa: mapa,
            estadoCTC: estadoCTC
        });
    } catch (error) {
        console.error('Error API estado:', error.message);
        res.status(500).json({ error: 'Error obteniendo estado del sistema' });
    }
});


app.listen(PORT, () => {
    console.log(`Frontend EasyCab corriendo en http://localhost:${PORT}`);
});
