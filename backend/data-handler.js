const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const { parse, differenceInDays, isValid } = require('date-fns');

const dataDir = path.join(__dirname, '../data');

function getOpenSalesOrders(user, callback) {
    const results = [];
    const filePath = path.join(dataDir, 'Open_Sales_Order.csv');

    if (!fs.existsSync(filePath)) {
        return callback(null, []);
    }

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            const isMatch = user.role === 'admin' || row['DSR Name'] === user.filter_name;
            const soDate = parse(row['SO Date'], 'dd/MM/yyyy', new Date());
            const isRecent = isValid(soDate) && differenceInDays(new Date(), soDate) <= 2;

            if (isMatch && isRecent) {
                results.push(row);
            }
        })
        .on('end', () => {
            const grouped = results.reduce((acc, row) => {
                const customer = row['Customer Name'];
                if (!acc[customer]) {
                    acc[customer] = {
                        customerName: customer,
                        orderQty: 0,
                        products: []
                    };
                }
                acc[customer].orderQty += parseFloat(row['Order Qty']);
                acc[customer].products.push({
                    productDescription: row['Product Description'],
                    orderQty: parseFloat(row['Order Qty'])
                });
                return acc;
            }, {});
            callback(null, Object.values(grouped));
        })
        .on('error', (error) => callback(error));
}

function getBillingDetails(user, callback) {
    const results = [];
    fs.readdir(dataDir, (err, files) => {
        if (err) {
            return callback(err);
        }
        const quarterlyFiles = files.filter(file => file.match(/^q\d.*\.csv$/));
        let filesProcessed = 0;

        if (quarterlyFiles.length === 0) {
            return callback(null, []);
        }

        quarterlyFiles.forEach(file => {
            fs.createReadStream(path.join(dataDir, file))
                .pipe(csv())
                .on('data', (row) => {
                    const isMatch = user.role === 'admin' || row['Sales Executive Name'] === user.filter_name;
                    const invoiceDate = parse(row['Invoice Date'], 'yyyy-MM-dd', new Date());
                    const isRecent = isValid(invoiceDate) && differenceInDays(new Date(), invoiceDate) <= 6;

                    if (isMatch && isRecent) {
                        results.push(row);
                    }
                })
                .on('end', () => {
                    filesProcessed++;
                    if (filesProcessed === quarterlyFiles.length) {
                        const grouped = results.reduce((acc, row) => {
                            const customer = row['Customer Name'];
                            if (!acc[customer]) {
                                acc[customer] = {
                                    customerName: customer,
                                    invoices: []
                                };
                            }
                            acc[customer].invoices.push({
                                invoiceNo: row['Invoice No'],
                                invoiceDate: row['Invoice Date'],
                                productVolume: parseFloat(row['Product Volume']),
                                products: [{
                                    productName: row['Product Name'],
                                    productVolume: parseFloat(row['Product Volume'])
                                }]
                            });
                            return acc;
                        }, {});
                        callback(null, Object.values(grouped));
                    }
                })
                .on('error', (error) => callback(error));
        });
    });
}

function searchInvoices(user, invoiceNo, callback) {
    const results = [];
    fs.readdir(dataDir, (err, files) => {
        if (err) {
            return callback(err);
        }
        const quarterlyFiles = files.filter(file => file.match(/^q\d.*\.csv$/));
        let filesProcessed = 0;

        if (quarterlyFiles.length === 0) {
            return callback(null, []);
        }

        quarterlyFiles.forEach(file => {
            fs.createReadStream(path.join(dataDir, file))
                .pipe(csv())
                .on('data', (row) => {
                    const isMatch = user.role === 'admin' || row['Sales Executive Name'] === user.filter_name;
                    if (isMatch && row['Invoice No'] === invoiceNo) {
                        results.push(row);
                    }
                })
                .on('end', () => {
                    filesProcessed++;
                    if (filesProcessed === quarterlyFiles.length) {
                        if (results.length > 0) {
                            const grouped = results.reduce((acc, row) => {
                                const invoiceNum = row['Invoice No'];
                                if (!acc[invoiceNum]) {
                                    acc[invoiceNum] = {
                                        customerName: row['Customer Name'],
                                        invoiceNo: invoiceNum,
                                        invoiceDate: row['Invoice Date'],
                                        totalProductVolume: 0,
                                        products: []
                                    };
                                }
                                acc[invoiceNum].totalProductVolume += parseFloat(row['Product Volume']);
                                acc[invoiceNum].products.push({
                                    productName: row['Product Name'],
                                    productVolume: parseFloat(row['Product Volume'])
                                });
                                return acc;
                            }, {});
                            callback(null, Object.values(grouped));
                        } else {
                            callback(null, []);
                        }
                    }
                })
                .on('error', (error) => callback(error));
        });
    });
}

module.exports = {
    getOpenSalesOrders,
    getBillingDetails,
    searchInvoices
};
