import sqlite3
import pandas as pd

conn = sqlite3.Connection('data.sqlite')

q = """
SELECT
    customerNumber,
    contactLastName,
    contactFirstName
FROM customers
WHERE customerNumber IN (SELECT customerNumber
                     FROM orders 
                     WHERE orderDate = '2003-01-31')
;
"""
print(pd.read_sql(q, conn))

q = """
SELECT
    productName, COUNT(orderNumber) AS numberOrders, SUM(quantityOrdered) AS totalUnitsSold
FROM products
JOIN orderdetails
    USING (productCode)
GROUP BY productName
ORDER BY totalUnitsSold DESC
;
"""
print(pd.read_sql(q, conn))

q = """
SELECT
    productName, COUNT(DISTINCT customerNumber) AS numPurchasers
FROM products
JOIN orderdetails
    USING (productCode)
JOIN orders
    USING(orderNumber)
GROUP BY productName
ORDER BY numPurchasers DESC
;
"""
print(pd.read_sql(q, conn))

q = """
SELECT
    DISTINCT employeeNumber, firstName, lastName, o.city, officeCode
FROM employees AS e
JOIN offices AS o
    USING(officeCode)
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders
    USING(customerNumber)
JOIN orderdetails
    USING(orderNumber)
WHERE productCode IN (SELECT
            productCode
        FROM products
        JOIN orderdetails
            USING (productCode)
        JOIN orders
            USING(orderNumber)
        GROUP BY productCode
        HAVING COUNT(DISTINCT customerNumber) < 20)
;
"""
print(pd.read_sql(q, conn))

q = """
SELECT
    employeeNumber, firstName, lastName, COUNT(customerNumber)
FROM employees AS e
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY employeeNumber
HAVING AVG(creditLimit) > 15000
;
"""
print(pd.read_sql(q, conn))

conn.close()