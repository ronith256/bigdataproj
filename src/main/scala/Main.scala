import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MySQLSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("bankloan")
      .master("local[*]")  // Add this line
      .getOrCreate()

    // connect mysql using jdbc database name is bankloaddb and table name is bankloan
    val loan = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/bankloandb")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "bankloan")
      .option("user", "siddharth")
      .option("password", "Best#123")
      .load()

    val df = loan.toDF()

    // print dataframe schema
    df.printSchema()

    // print dataframe show first 10 rows
    df.show(10)

//  ----------------------------------------------------------------------------------------------------------------------------------------------------
    /* Key Performances of the Bank */

// ----------------------------- loan application metrics -----------------------------

    // count the number of id as total_loan_application
    // select count(id) as total_loan_application from bankloan
    val total_loan_application = df.groupBy("id").agg(count("id") as "total_loan_application")
    total_loan_application.show()

    // MTD stands for month to date
    // select count(id) as total_loan_application from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MTD_loan_application_in_december = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .groupBy("id")
      .agg(count("id") as "total_loan_application_in_december")
    println("count_MTD_loan_application_in_december: " + MTD_loan_application_in_december.count())
    MTD_loan_application_in_december.show()

    // PMTD stands for previous month to date
    // select count(id) as total_loan_application from bankloan where month(issue_date) = 11 and year(issue_date) = 2021
    val PMTD_loan_application_in_november = df.filter(month(col("issue_date")) === 11 && year(col("issue_date")) === 2021)
      .groupBy("id")
      .agg(count("id") as "total_loan_application_in_november")
    println("count_PMTD_loan_application_in_november: " + PMTD_loan_application_in_november.count())
    PMTD_loan_application_in_november.show()

    // calculate MOM percentage
    // take the value of mtd subtract with pmtd and divide by pmtd and multiply by 100
    // select (count(id) - (select count(id) as total_loan_application from bankloan where month(issue_date) = 11 and year(issue_date) = 2021)) / (select count(id) as total_loan_application from bankloan where month(issue_date) = 11 and year(issue_date) = 2021) * 100 as mom_percentage from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MOM_percentage = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .agg(((count("id") - PMTD_loan_application_in_november.count()) / PMTD_loan_application_in_november.count()) * 100 as "MOM_percentage")
    MOM_percentage.show()

// ----------------------------- loan metrics -----------------------------

    // total loan amount
    // select sum(loan_amount) as total_funded_ammount from bankloan
    val total_funded_amount = df.agg(sum("loan_amount") as "total_funded_amount")
    total_funded_amount.show()

    // total funded amount this december
    // select sum(loan_amount) as total_funded_amount from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val total_funded_amount_december = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .agg(sum("loan_amount") as "total_funded_amount_december")
    total_funded_amount_december.show()

    // total funded amount this november
    // select sum(loan_amount) as total_funded_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021
    val total_funded_amount_november = df.filter(month(col("issue_date")) === 11 && year(col("issue_date")) === 2021)
      .agg(sum("loan_amount") as "total_funded_amount_november")
    total_funded_amount_november.show()

    // calculate MOM percentage
    // take the value of mtd subtract with pmtd and divide by pmtd and multiply by 100
    // select (sum(loan_amount) - (select sum(loan_amount) as total_funded_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021)) / (select sum(loan_amount) as total_funded_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021) * 100 as mom_percentage from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MOM_percentage_loan_amount = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .agg(((sum("loan_amount") - total_funded_amount_november.first().getLong(0).toDouble) / total_funded_amount_november.first().getLong(0).toDouble) * 100 as "MOM_percentage_loan_amount")
    MOM_percentage_loan_amount.show()      
    
// ----------------------------- received amount metrics -----------------------------

    // total received amount
    // select sum(total_payment) as total_received_amount from bankloan
    val total_received_amount = df.agg(sum("total_payment") as "total_received_amount")
    total_received_amount.show()

    // total received amount this december
    // select sum(total_payment) as total_received_amount from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val total_received_amount_december = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .agg(sum("total_payment") as "total_received_amount_december")
    total_received_amount_december.show()

    // total received amount this november
    // select sum(total_payment) as total_received_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021
    val total_received_amount_november = df.filter(month(col("issue_date")) === 11 && year(col("issue_date")) === 2021)
      .agg(sum("total_payment") as "total_received_amount_november")
    total_received_amount_november.show()

    // calculate MOM percentage
    // take the value of mtd subtract with pmtd and divide by pmtd and multiply by 100
    // select (sum(total_payment) - (select sum(total_payment) as total_received_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021)) / (select sum(total_payment) as total_received_amount from bankloan where month(issue_date) = 11 and year(issue_date) = 2021) * 100 as mom_percentage from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MOM_percentage_received_amount = df.filter(month(col("issue_date")) === 12 && year(col("issue_date"))==2021)
      .agg(((sum("total_payment") - total_received_amount_november.first().getLong(0).toDouble)/ total_received_amount_november.first().getLong(0).toDouble)*100 as "MOM_percentage_received_amount")

// ----------------------------- Interest metrics -----------------------------

    // find average interest rate in percentage upto 2 decimal places
    // select round(avg(interest_rate) * 100, 5) as average_interest_rate from bankloan
    val average_interest_rate = df.agg(round(avg("int_rate") * 100, 5) as "average_interest_rate")
    average_interest_rate.show()

    // find MTD average interest rate in percentage upto 2 decimal places
    // select round(avg(interest_rate) * 100, 5) as average_interest_rate from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MTD_average_interest_rate = df.filter(month(col("issue_date")) === 12 && year(col("issue_date")) === 2021)
      .agg(round(avg("int_rate") * 100, 5) as "MTD_average_interest_rate")

    // find PMTD average interest rate in percentage upto 2 decimal places
    // select round(avg(interest_rate) * 100, 5) as average_interest_rate from bankloan where month(issue_date) = 11 and year(issue_date) = 2021
    val PMTD_average_interest_rate = df.filter(month(col("issue_date")) === 11 && year(col("issue_date")) === 2021)
      .agg(round(avg("int_rate") * 100, 5) as "PMTD_average_interest_rate")

    // calculate MOM percentage
    // take the value of mtd subtract with pmtd and divide by pmtd and multiply by 100
    // select (round(avg(interest_rate) * 100, 5) - (select round(avg(interest_rate) * 100, 5) as average_interest_rate from bankloan where month(issue_date) = 11 and year(issue_date) = 2021)) / (select round(avg(interest_rate) * 100, 5) as average_interest_rate from bankloan where month(issue_date) = 11 and year(issue_date) = 2021) * 100 as mom_percentage from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MOM_percentage_interest_rate = (MTD_average_interest_rate.first().getDouble(0) - PMTD_average_interest_rate.first().getDouble(0)) / PMTD_average_interest_rate.first().getDouble(0) * 100
    println(s"MOM Percentage Interest Rate: $MOM_percentage_interest_rate")

// ----------------------------- Debt to income ratio metrics -----------------------------

    // Average Debt to income ratio 
    // select round(avg(dti), 5) as average_debt_to_income_ratio from bankloan
    val average_debt_to_income_ratio = df.agg(round(avg("dti"), 5) as "average_debt_to_income_ratio")
    average_debt_to_income_ratio.show()

    // Average MTD Debt to income ratio on December month
    // select round(avg(dti), 5) as average_debt_to_income_ratio from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MTD_average_debt_to_income_ratio = df.filter(date_format(col("issue_date"), "MM") === "12" && date_format(col("issue_date"), "yyyy") === "2021")
                                         .agg(round(avg("dti"), 5).alias("MTD_average_debt_to_income_ratio"))    
    MTD_average_debt_to_income_ratio.show()

    // Average PMTD Debt to income ratio on November month
    // select round(avg(dti), 5)*100 as average_debt_to_income_ratio from bankloan where month(issue_date) = 11 and year(issue_date) = 2021
    val PMTD_average_debt_to_income_ratio = df.filter(month(col("issue_date")) === 11 && year(col("issue_date")) === 2021)
      .agg(round(avg("dti"), 4)*100 as "PMTD_average_debt_to_income_ratio")
    PMTD_average_debt_to_income_ratio.show()

    // calculate MOM percentage
    // take the value of mtd subtract with pmtd and divide by pmtd and multiply by 100
    // select (round(avg(dti), 5) - (select round(avg(dti), 5) as average_debt_to_income_ratio from bankloan where month(issue_date) = 11 and year(issue_date) = 2021)) / (select round(avg(dti), 5) as average_debt_to_income_ratio from bankloan where month(issue_date) = 11 and year(issue_date) = 2021) * 100 as mom_percentage from bankloan where month(issue_date) = 12 and year(issue_date) = 2021
    val MOM_percentage_debt_to_income_ratio = (MTD_average_debt_to_income_ratio.first().getDouble(0) - PMTD_average_debt_to_income_ratio.first().getDouble(0)) / PMTD_average_debt_to_income_ratio.first().getDouble(0) * 100
    println(s"MOM Percentage Debt to Income Ratio: $MOM_percentage_debt_to_income_ratio")

    // monthly average debt to income ratio
    // select
    //    month(issue_date) as month,
    //    round(avg(dti), 5) as average_debt_to_income_ratio
    // from bankloan
    // group by month(issue_date)
    // order by month(issue_date)
    val monthly_average_debt_to_income_ratio = df.withColumn("month", month(col("issue_date")))
      .groupBy("month")
      .agg(round(avg("dti"), 5) as "average_debt_to_income_ratio")
      .orderBy("month")
    monthly_average_debt_to_income_ratio.show()

//  ----------------------------------------------------------------------------------------------------------------------------------------------------
  /* Good Loan vs Bad Loan statuses */

  // ----------------------------- good loan metrics -----------------------------

    // loan status (select all the unique loan status)
    // select distinct(loan_status) from bankloan
    val distinct_loan_status = df.select("loan_status").distinct()
    distinct_loan_status.show()

    // total percentage for good loan status
    // select (count(case when loan_status = "Fully Paid" or loan_status = "Current" then id end)*100) / count(id) as good_loan_percentage from bankloan
    val good_loan_percentage = df.agg((count(when(col("loan_status") === "Fully Paid" || col("loan_status") === "Current", col("id"))) * 100) / count(col("id")) as "good_loan_percentage")
    good_loan_percentage.show()

    // good loan applications
    // select count(case when loan_status = "Fully Paid" or loan_status = "Current" then id end) as good_loan_application from bankloan
    val good_loan_application = df.agg(count(when(col("loan_status") === "Fully Paid" || col("loan_status") === "Current", col("id"))) as "good_loan_application")
    good_loan_application.show()

    // good loan funded amount
    // select sum(case when loan_status = "Fully Paid" or loan_status = "Current" then loan_amount end) as good_loan_funded_amount from bankloan
    val good_loan_funded_amount = df.agg(sum(when(col("loan_status") === "Fully Paid" || col("loan_status") === "Current", col("loan_amount"))) as "good_loan_funded_amount")
    good_loan_funded_amount.show()

    // good loan received amount
    // select sum(case when loan_status = "Fully Paid" or loan_status = "Current" then total_payment end) as good_loan_received_amount from bankloan
    val good_loan_received_amount = df.agg(sum(when(col("loan_status") === "Fully Paid" || col("loan_status") === "Current", col("total_payment"))) as "good_loan_received_amount")
    good_loan_received_amount.show()

  // ----------------------------- bad loan metrics -----------------------------

    // total percentage for bad loan status
    // select (count(case when loan_status = "Charged Off" then id end)*100) / count(id) as bad_loan_percentage from bankloan
    val bad_loan_percentage = df.agg((count(when(col("loan_status") === "Charged Off", col("id"))) * 100) / count(col("id")) as "bad_loan_percentage")
    bad_loan_percentage.show()

    // bad loan applications
    // select count(case when loan_status = "Charged Off" then id end) as bad_loan_application from bankloan
    val bad_loan_application = df.agg(count(when(col("loan_status") === "Charged Off", col("id"))) as "bad_loan_application")
    bad_loan_application.show()

    // bad loan funded amount
    // select sum(case when loan_status = "Charged Off" then loan_amount end) as bad_loan_funded_amount from bankloan
    val bad_loan_funded_amount = df.agg(sum(when(col("loan_status") === "Charged Off", col("loan_amount"))) as "bad_loan_funded_amount")
    bad_loan_funded_amount.show()

    // loan status grid 
    // select loan_status, count(id) as loancount, sum(totoal_payment) as total_amount_received, sum(loan_amount) as total_funded_amount, avg(int_rate * 100) as interest_rate, avg(dti * 100) as dti from bankloan groupby loan_status
    val loan_status_grid = df.groupBy("loan_status")
      .agg(count("id") as "loancount", sum("total_payment") as "total_amount_received", sum("loan_amount") as "total_funded_amount", avg("int_rate") * 100 as "interest_rate", avg("dti") * 100 as "dti")
    loan_status_grid.show()

// -------------------------------------------------------------------------------------

    // MTD loan status grid
    // select 
    //    loan_status, 
    //    count(id) as loancount, 
    //    sum(totoal_payment) as total_amount_received, 
    //    sum(loan_amount) as total_funded_amount, 
    //    avg(int_rate * 100) as interest_rate, 
    //    avg(dti * 100) as dti from bankloan 
    // where 
    //    month(issue_date) = 12 and year(issue_date) = 2021 
    // groupby 
    //    loan_status
    val MTD_loan_status_grid = df.filter(date_format(col("issue_date"), "MM") === "12" && date_format(col("issue_date"), "yyyy") === "2021")
      .groupBy("loan_status")
      .agg(count("id") as "loancount", sum("total_payment") as "total_amount_received", sum("loan_amount") as "total_funded_amount", avg("int_rate") * 100 as "interest_rate", avg("dti") * 100 as "dti")
    MTD_loan_status_grid.show()

    // PMTD loan status grid
    // select loan_status, count(id) as loancount, sum(totoal_payment) as total_amount_received, sum(loan_amount) as total_funded_amount, avg(int_rate * 100) as interest_rate, avg(dti * 100) as dti from bankloan where month(issue_date) = 11 and year(issue_date) = 2021 groupby loan_status
    val PMTD_loan_status_grid = df.filter(date_format(col("issue_date"), "MM") === "11" && date_format(col("issue_date"), "yyyy") === "2021")
      .groupBy("loan_status")
      .agg(count("id") as "loancount", sum("total_payment") as "total_amount_received", sum("loan_amount") as "total_funded_amount", avg("int_rate") * 100 as "interest_rate", avg("dti") * 100 as "dti")
    PMTD_loan_status_grid.show()

    // -------------------------------------------------------------------------------------
    /* charts time series */

    // SELECT 
    //    MONTH(issue_date),
    //    MONTHNAME(issue_date) AS month_name, 
    //    COUNT(id) AS total_loan_applications, 
    //    SUM(loan_amount) AS total_funded_amount,
    //    SUM(total_payment) AS total_received_amount
    // FROM bankloan
    // GROUP BY MONTH(issue_date), MONTHNAME(issue_date)
    // ORDER BY MONTH(issue_date)
    val monthly_trend = df.withColumn("month", month(col("issue_date")))
                          .withColumn("month_name", date_format(col("issue_date"), "MMMM"))
                          .groupBy("month", "month_name")
                          .agg(count("id").alias("total_loan_applications"), 
                               sum("loan_amount").alias("total_funded_amount"), 
                               sum("total_payment").alias("total_received_amount"))
                          .orderBy("month")
    monthly_trend.show()
      
    // regional trend by state 
    // select 
    //    address_state,
    //    count(id) as total_loan_applications, 
    //    sum(loan_amount) as total_funded_amount,
    //    sum(total_payment) as total_received_amount
    // from bankloan
    // group by address_state
    // order by sum(loan_amount) desc
    val regional_trend = df.groupBy("address_state")
                            .agg(count("id").alias("total_loan_applications"), 
                                 sum("loan_amount").alias("total_funded_amount"), 
                                 sum("total_payment").alias("total_received_amount"))
                            .orderBy(sum("loan_amount").desc)
    regional_trend.show()

    // term analysis
    // select
    //    term,
    //    count(id) as total_loan_applications,
    //    sum(loan_amount) as total_funded_amount,
    //    sum(total_payment) as total_received_amount
    // from bankloan
    // group by term
    // order by term
    val term_analysis = df.groupBy("term")
                          .agg(count("id").alias("total_loan_applications"), 
                               sum("loan_amount").alias("total_funded_amount"), 
                               sum("total_payment").alias("total_received_amount"))
                          .orderBy("term")
    term_analysis.show()

    // employment length analysis
    // select
    //    emp_length,
    //    count(id) as total_loan_applications,
    //    sum(loan_amount) as total_funded_amount,
    //    sum(total_payment) as total_received_amount
    // from bankloan
    // group by emp_length
    // order by emp_length
    val employment_length_analysis = df.groupBy("emp_length")
                                        .agg(count("id").alias("total_loan_applications"), 
                                             sum("loan_amount").alias("total_funded_amount"), 
                                             sum("total_payment").alias("total_received_amount"))
                                        .orderBy("emp_length")
    employment_length_analysis.show()

    // loan purpose analysis
    // select
    //    purpose,
    //    count(id) as total_loan_applications,
    //    sum(loan_amount) as total_funded_amount,
    //    sum(total_payment) as total_received_amount
    // from bankloan
    // group by purpose
    // order by sum(loan_amount) desc
    val loan_purpose_analysis = df.groupBy("purpose")
                                    .agg(count("id").alias("total_loan_applications"), 
                                         sum("loan_amount").alias("total_funded_amount"), 
                                         sum("total_payment").alias("total_received_amount"))
                                    .orderBy(sum("loan_amount").desc)
    loan_purpose_analysis.show()

    // loan analysis based on home ownership
    // select
    //    home_ownership,
    //    count(id) as total_loan_applications,
    //    sum(loan_amount) as total_funded_amount,
    //    sum(total_payment) as total_received_amount
    // from bankloan
    // group by home_ownership
    // order by sum(loan_amount) desc
    val home_ownership_analysis = df.groupBy("home_ownership")
                                      .agg(count("id").alias("total_loan_applications"), 
                                           sum("loan_amount").alias("total_funded_amount"), 
                                           sum("total_payment").alias("total_received_amount"))
                                      .orderBy(sum("loan_amount").desc)
    home_ownership_analysis.show()

  }
}