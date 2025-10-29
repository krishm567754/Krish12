const express = require('express');
const session = require('express-session');
const path = require('path');
const dataHandler = require('./data-handler');
const crypto = require('crypto');

const app = express();
const port = 3000;

app.use(session({
    secret: crypto.randomBytes(64).toString('hex'),
    resave: false,
    saveUninitialized: true,
    cookie: { secure: false }
}));

app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, '../frontend')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/index.html'));
});

app.post('/login', (req, res) => {
    const { username, password } = req.body;
    // For simplicity, user authentication remains with users.csv.
    // In a real-world scenario, this should also be more robust.
    const users = [];
    const fs = require('fs');
    const csv = require('csv-parser');
    fs.createReadStream(path.join(__dirname, '../data/users.csv'))
        .pipe(csv())
        .on('data', (data) => users.push(data))
        .on('end', () => {
            const user = users.find(u => u.username === username && u.password === password);
            if (user) {
                req.session.user = {
                    username: user.username,
                    role: user.role,
                    filter_name: user.filter_name
                };
                res.redirect('/dashboard.html');
            } else {
                res.send('Invalid username or password');
            }
        });
});

const checkAuth = (req, res, next) => {
    if (req.session.user) {
        next();
    } else {
        res.redirect('/');
    }
};

app.get('/dashboard', checkAuth, (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/dashboard.html'));
});

app.get('/api/sales-orders', checkAuth, (req, res) => {
    dataHandler.getOpenSalesOrders(req.session.user, (err, data) => {
        if (err) {
            return res.status(500).send('Error reading data');
        }
        res.json(data);
    });
});

app.get('/api/billing-details', checkAuth, (req, res) => {
    dataHandler.getBillingDetails(req.session.user, (err, data) => {
        if (err) {
            return res.status(500).send('Error reading data');
        }
        res.json(data);
    });
});

app.get('/api/invoice-search', checkAuth, (req, res) => {
    const { invoiceNo } = req.query;
    if (!invoiceNo) {
        return res.status(400).send('Invoice number is required');
    }
    dataHandler.searchInvoices(req.session.user, invoiceNo, (err, data) => {
        if (err) {
            return res.status(500).send('Error reading data');
        }
        res.json(data);
    });
});

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});
