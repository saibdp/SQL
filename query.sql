CREATE TABLE customers (
           customer_id INT PRIMARY KEY,
           first_name VARCHAR(50),
           last_name VARCHAR(50),
           address VARCHAR(100),
           city VARCHAR(50),
           state VARCHAR(50),
           zip VARCHAR(20)
       );
       
CREATE TABLE accounts (
	account_id INT PRIMARY KEY,
	customer_id INT,
	account_type VARCHAR(50),
    balance DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
CREATE TABLE transactions (
           transaction_id INT PRIMARY KEY,
           account_id INT,
           transaction_date DATE,
           transaction_amount DECIMAL(10, 2),
           transaction_type VARCHAR(50),
           FOREIGN KEY (account_id) REFERENCES accounts(account_id)
       );
CREATE TABLE loans (
           loan_id INT PRIMARY KEY,
           customer_id INT,
           loan_amount DECIMAL(10, 2),
           interest_rate DECIMAL(5, 2),
           loan_term INT,
           FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
       );
CREATE TABLE loan_payments (
           payment_id INT PRIMARY KEY,
           loan_id INT,
           payment_date DATE,
           payment_amount DECIMAL(10, 2),
           FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
       );
BULK INSERT [dbo].[customers]
FROM '\customers .csv'
WITH
(
FORMAT = 'csv',
FirstRow = 2
);

--Created staging account since there is a confilict in foriegn key
BULK INSERT [dbo].[accounts_staging_table]
FROM '\accounts.csv'
WITH
(
FORMAT = 'csv',
FirstRow = 2
);

BULK INSERT [dbo].[transactions_staging_table]
FROM '\transactions.csv'
WITH
(
FORMAT = 'csv',
FirstRow = 2
);

BULK INSERT [dbo].[loans_staging_table]
FROM '\loans.csv'
WITH
(
FORMAT = 'csv',
FirstRow = 2
);

BULK INSERT [dbo].[loan_payments_staging_table]
FROM '\loan_payments.csv'
WITH
(
FORMAT = 'csv',
FirstRow = 2
);

-- Selected the data from tables with values matching in the foriegn key so that both primary and foriegn key are intact
INSERT INTO dbo.accounts (account_id,customer_id, account_type, balance)
SELECT *
FROM dbo.accounts_staging_table s
INNER JOIN dbo.customers c ON s.customer_id = c.customer_id;

INSERT INTO dbo.transactions (transaction_id,account_id, transaction_date, transaction_amount,transaction_type)
SELECT *
FROM dbo.transactions_staging_table s
INNER JOIN dbo.accounts a ON s.account_id = a.account_id;


INSERT INTO dbo.loans (loan_id,customer_id,loan_amount,interest_rate,loan_term)
SELECT *
FROM dbo.loans_staging_table s
INNER JOIN dbo.customers c ON s.customer_id = c.customer_id;

INSERT INTO dbo.loan_payments (payment_id,loan_id,payment_date,payment_amount)
SELECT *
FROM dbo.transactions_staging_table s
INNER JOIN dbo.loans l ON s.loan_id = l.loan_id;

--4. Data Exploration
-- - Step 4.1: Write query to retrieve all customer information:
select * from [dbo].[customers] as c 
join [dbo].[accounts] a on a.customer_id = c.customer_id
join [dbo].[transactions] t on t.account_id = a.account_id
join [dbo].[loans] as l on l.customer_id = c.customer_id
join [dbo].[loan_payments] as lp on lp.loan_id = l.loan_id;

--   - Step 4.2: Query accounts for a specific customer:
select * from [dbo].[customers] as c 
join [dbo].[accounts] a on a.customer_id = c.customer_id
join [dbo].[transactions] t on t.account_id = a.account_id
join [dbo].[loans] as l on l.customer_id = c.customer_id
join [dbo].[loan_payments] as lp on lp.loan_id = l.loan_id
where c.customer_id = 13;

--- Step 4.3: Find the customer name and account balance for each account
select c.first_name,c.last_name, SUM(a.balance) as total_acc_balance from [dbo].[customers] as c 
join [dbo].[accounts] a on a.customer_id = c.customer_id
join =[dbo].[transactions] t on t.account_id = a.account_id
join [dbo].[loans] as l on l.customer_id = c.customer_id
join [dbo].[loan_payments] as lp on lp.loan_id = l.loan_id
GROUP by c.first_name,c.last_name;

--Step 4.4: Analyze customer loan balances:
SELECT l.customer_id, l.loan_amount - lp.payment_amount as loan_balance from loans as l
JOIN (select loan_id, payment_amount from loan_payments
GROUP BY loan_id, payment_amount
) as lp on lp.loan_id = l.loan_id;


--Step 4.5: List all customers who have made a transaction in the 2024-03
select c.customer_id,first_name, last_name from [dbo].[customers] as c 
join [dbo].[accounts] a on a.customer_id = c.customer_id
join [dbo].[transactions] t on t.account_id = a.account_id
join [dbo].[loans] as l on l.customer_id = c.customer_id
join [dbo].[loan_payments] as lp on lp.loan_id = l.loan_id
WHERE t.transaction_date BETWEEN '2024-03-01' AND '2024-03-31'
group by c.customer_id,first_name, last_name;

-- 5. Aggregation and Insights
-- - Step 5.1: Calculate the total balance across all accounts for each customer:
SELECT c.customer_id, c.first_name, a.account_type, SUM(a.balance) as total_balance from accounts as a
join customers as c on c.customer_id = a.account_id
group by c.customer_id,c.first_name,a.account_type;

--Step 5.2: Calculate the average loan amount for each loan term:
SELECT loan_term, AVG(loan_amount) as avg_loan_amount from loans
group by loan_term;

--Step 5.3: Find the total loan amount and interest across all loans:
SELECT interest_rate, SUM(loan_amount) as total_loan_amount from loans
GROUP by interest_rate;

--Step 5.4: Find the most frequent transaction type
SELECT top 1 transaction_type, count(transaction_type) as count_of_transactions FROM transactions
GROUP BY transaction_type
ORDER BY count_of_transactions DESC;

--Step 5.5: Analyze transactions by account and transaction type:
SELECT account_id, transaction_type, SUM(transaction_amount) as total_transaction_amount from transactions
GROUP BY account_id, transaction_type;

--6. Advanced Analysis
--Step 6.1: Create a view of active loans with payments greater than $1000:
CREATE VIEW active_loans as 
SELECT l.loan_id, l.customer_id,l.loan_amount, l.interest_rate, l.loan_term, lp.payment_amount, l.loan_amount - lp.payment_amount as loan_balance from loans as l
JOIN (select loan_id, payment_amount from loan_payments
group by loan_id, payment_amount) as lp on lp.loan_id = l.loan_id
WHERE lp.payment_amount > 1000;

--Step 6.2: Create an index on `transaction_date` in the `transactions` table for performance optimization:
CREATE INDEX idx_transaction_date
ON transactions (transaction_date);