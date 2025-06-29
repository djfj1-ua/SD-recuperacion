const express = require('express');
const axios = require('axios');
const app = express();
const PORT = 3000;
const fs = require('fs');

app.set('view engine', 'ejs');
app.use(express.static('public'));

// Ruta principal
app.get('/', async (req, res) => {
    try {
        const centralEstado = await axios.get('http://127.0.0.1:8000/estado');
        const ctcEstado = await axios.get('http://192.168.1.40:5000/consulta');

        const { taxis, clientes, mapa } = centralEstado.data;
        const estadoCTC = ctcEstado.data.estado;

        let auditoriaText = '';
        try {
            auditoriaText = fs.readFileSync('../auditoria.log', 'utf-8');
        } catch (err) {
            console.error('Error leyendo el log de auditoría:', err.message);
        }

        res.render('index', {
            taxis: taxis,
            clientes: clientes,
            mapa: mapa,
            estadoCTC: estadoCTC,
            auditoria: auditoriaText
        });

    } catch (error) {
        console.error('Error obteniendo datos:', error.message);
        res.send('<h1>Error conectando a EC_Central o CTC</h1>');
    }
});

app.get('/auditoria', async (req, res) => {
    try {
        const auditoriaResponse = await axios.get('http://127.0.0.1:8000/auditoria');  // Cambia el puerto si usas otro
        const contenido = auditoriaResponse.data.auditoria;
        res.render('auditoria', { contenido });
    } catch (error) {
        console.error('Error obteniendo la auditoría:', error.message);
        res.send('<h1>Error conectando al API de auditoría</h1>');
    }
});

app.get('/api/estado', async (req, res) => {
    try {
        const centralEstado = await axios.get('http://127.0.0.1:8000/estado');
        const ctcEstado = await axios.get('http://192.168.1.40:5000/consulta');

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


app.listen(PORT, '0.0.0.0', () => {
    console.log(`Frontend EasyCab corriendo en http://0.0.0.0:${PORT}`);
});

