CREATE DATABASE IF NOT EXISTS bankloan;
GRANT ALL PRIVILEGES ON bankloan.* TO 'siddharth'@'%';
FLUSH PRIVILEGES;

USE bankloan;

-- Table creation
CREATE TABLE IF NOT EXISTS bankloan (
  id BIGINT NOT NULL,
  address_state VARCHAR(100),
  application_type VARCHAR(100),
  emp_length VARCHAR(100),
  emp_title VARCHAR(100),
  grade VARCHAR(100),
  home_ownership VARCHAR(100),
  issue_date DATE,
  last_credit_pull_date DATE,
  last_payment_date DATE,
  loan_status VARCHAR(100),
  next_payment_date DATE,
  member_id BIGINT,
  purpose VARCHAR(100),
  sub_grade VARCHAR(100),
  term VARCHAR(100),
  verification_status VARCHAR(100),
  annual_income DOUBLE,
  dti DOUBLE,
  installment DOUBLE,
  int_rate DOUBLE,
  loan_amount BIGINT,
  total_acc BIGINT,
  total_payment BIGINT,
  PRIMARY KEY (id)
);

-- Data import
LOAD DATA INFILE '/var/lib/mysql-files/financial_loan.csv'
INTO TABLE bankloan
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;