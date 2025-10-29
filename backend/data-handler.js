const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs').promises; // Using promises version of fs
const { parse, differenceInDays, isValid } = require('date-fns');

const dataDir = path.join(__dirname, '../data');

async function readExcelFile(filePath) {
    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.worksheets[0];
        if (!worksheet) return [];

        const data = [];
        const header = worksheet.getRow(1).values;
        // The header array from exceljs can be sparse, so we clean it up.
        const cleanedHeader = header.filter(h => h);

        worksheet.eachRow((row, rowNumber) => {
            if (rowNumber > 1) {
                let rowData = {};
                row.values.forEach((value, index) => {
                    if (cleanedHeader[index - 1]) { // index is 1-based
                        rowData[cleanedHeader[index - 1]] = value;
                    }
                });
                data.push(rowData);
            }
        });
        return data;
    } catch (error) {
        // If a file is invalid or unreadable, we'll log it and return an empty array.
        console.error(`Error reading file ${filePath}:`, error);
        return [];
    }
}

async function getOpenSalesOrders(user, callback) {
    try {
        const filePath = path.join(dataDir, 'Open_Sales_Order_29Oct2025.xlsx');
        const data = await readExcelFile(filePath);
        const results = data.filter(row => {
            const isMatch = user.role === 'admin' || row['DSR Name'] === user.filter_name;
            const soDateRaw = row['SO Date'];
            const soDate = soDateRaw instanceof Date ? soDateRaw : parse(soDateRaw, 'dd/MM/yyyy', new Date());
            const isRecent = isValid(soDate) && differenceInDays(new Date(), soDate) <= 2;
            return isMatch && isRecent;
        });

        const grouped = results.reduce((acc, row) => {
            const customer = row['Customer Name'];
            if (!acc[customer]) {
                acc[customer] = { customerName: customer, orderQty: 0, products: [] };
            }
            acc[customer].orderQty += parseFloat(row['Order Qty'] || 0);
            acc[customer].products.push({
                productDescription: row['Product Description'],
                orderQty: parseFloat(row['Order Qty'] || 0)
            });
            return acc;
        }, {});
        callback(null, Object.values(grouped));
    } catch (error) {
        callback(error);
    }
}

async function processQuarterlyFiles(user, filterLogic) {
    const allResults = [];
    const files = await fs.readdir(dataDir);
    const quarterlyFiles = files.filter(file => file.match(/^q\d.*\.xlsx$/i));

    for (const file of quarterlyFiles) {
        const filePath = path.join(dataDir, file);
        const data = await readExcelFile(filePath);
        const filteredData = data.filter(row => filterLogic(user, row));
        allResults.push(...filteredData);
    }
    return allResults;
}

async function getBillingDetails(user, callback) {
    try {
        const results = await processQuarterlyFiles(user, (usr, row) => {
            const isMatch = usr.role === 'admin' || row['Sales Executive Name'] === usr.filter_name;
            const invoiceDateRaw = row['Invoice Date'];
            const invoiceDate = invoiceDateRaw instanceof Date ? invoiceDateRaw : parse(invoiceDateRaw, 'yyyy-MM-dd', new Date());
            const isRecent = isValid(invoiceDate) && differenceInDays(new Date(), invoiceDate) <= 6;
            return isMatch && isRecent;
        });

        const grouped = results.reduce((acc, row) => {
            const customer = row['Customer Name'];
            if (!acc[customer]) {
                acc[customer] = { customerName: customer, invoices: [] };
            }
            acc[customer].invoices.push({
                invoiceNo: row['Invoice No'],
                invoiceDate: row['Invoice Date'],
                productVolume: parseFloat(row['Product Volume'] || 0),
                products: [{
                    productName: row['Product Name'],
                    productVolume: parseFloat(row['Product Volume'] || 0)
                }]
            });
            return acc;
        }, {});
        callback(null, Object.values(grouped));
    } catch (error) {
        callback(error);
    }
}

async function searchInvoices(user, invoiceNo, callback) {
    try {
        const results = await processQuarterlyFiles(user, (usr, row) => {
            const isMatch = usr.role === 'admin' || row['Sales Executive Name'] === usr.filter_name;
            return isMatch && row['Invoice No'] == invoiceNo;
        });

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
                acc[invoiceNum].totalProductVolume += parseFloat(row['Product Volume'] || 0);
                acc[invoiceNum].products.push({
                    productName: row['Product Name'],
                    productVolume: parseFloat(row['Product Volume'] || 0)
                });
                return acc;
            }, {});
            callback(null, Object.values(grouped));
        } else {
            callback(null, []);
        }
    } catch (error) {
        callback(error);
    }
}

module.exports = {
    getOpenSalesOrders,
    getBillingDetails,
    searchInvoices
};
