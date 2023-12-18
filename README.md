## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).


<!-- configuring MySQL -->

<!-- create database -->
CREATE DATABASE IF NOT EXISTS bankloan;
GRANT ALL PRIVILEGES ON bankloaddb.* TO '[your user name]'@'localhost';

eg: GRANT ALL PRIVILEGES ON bankloaddb.* TO 'siddharth'@'localhost'; 

<!-- create table -->
CREATE TABLE bankloan (
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

<!-- move file here if you dont get permission -->
LOAD DATA INFILE '/var/lib/mysql-files/financial_loan.csv' INTO TABLE bankloan FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


<!-- sbt use the normal stuff -->
sbt compile
sbt run

<!-- graphana -->
http://localhost:3000/login
admin/admin

<!-- connect the mysql database here -->
1) http://localhost:3000/datasources/new
2) mysql
3) bankloan
4) [username here]/[password here] --> eg: siddharth/mypassword
5) bankloan

Load the file from the folder graphana and import it to the graphana dashboard
