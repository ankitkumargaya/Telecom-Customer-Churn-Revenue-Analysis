create database Customer_Churn_Analysis;
use Customer_Churn_Analysis;

set sql_safe_updates=0;
 truncate table customers;

alter table customers
modify Churn_Date date null ;

alter table customers
rename column hurn_Status to Churn_Status;

CREATE TABLE customers (
    Customer_ID VARCHAR(100) PRIMARY KEY,
    Name VARCHAR(101),
    Gender VARCHAR(102),
    Age INT,
    City VARCHAR(50),
    State VARCHAR(50),
    Join_Date DATE,
    hurn_Status VARCHAR(10),
    Churn_Date DATE
);

CREATE TABLE subscriptions (
    Subscription_ID VARCHAR(100) PRIMARY KEY,
    Customer_ID VARCHAR(100),
    Plan_Name VARCHAR(101),
    Plan_Type VARCHAR(102),
    Start_Date DATE,
    End_Date DATE,
    Monthly_Charges NUMERIC(5 , 2 ),
    Total_Revenue NUMERIC(10 , 2 )
);

CREATE TABLE usage_i (
    Usage_ID VARCHAR(150) PRIMARY KEY,
    Customer_ID VARCHAR(100),
    Month_Year DATE,
    Total_Minutes_Used INT,
    Data_Used_GB NUMERIC(6 , 2 ),
    Complaints_Registered INT,
    Late_Payments INT
);

 

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customer.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    Customer_ID, 
    Name, 
    Gender, 
    Age, 
    City, 
    State, 
    @join_date_var, 
    Churn_Status, 
    @churn_date_var
)
SET
    Join_Date = STR_TO_DATE(NULLIF(@join_date_var, ''), '%Y-%m-%d'),
    Churn_Date = STR_TO_DATE(NULLIF(@churn_date_var, ''), '%Y-%m-%d');
    SET SESSION sql_mode = '';
    
    
    
select*from customers;
select*from  subscriptions;
select*from  usage_i;

create index idx_Total_Late_Payments on usage_i (Late_Payments);
create index idx_Age on customers(Age);
create index idx_name on customers(name(30));
-- Level_ 1: Basic Data Understanding:
-- 1)Total number of customers:

select count(name) as total_customers
from customers;
 
-- 2)Number of customers who churned vs active---
select churn_status, count(Customer_ID) as active_customers
from customers
where churn_status = 'Yes';

-- 3)Average age of customers
select avg(Age) as Average_age 
from customers;

-- 4)Top 5 states by total customers
select state,count(Customer_ID) as total_customers
from customers
group by state  
order by count(Customer_ID) desc limit 5;

-- )5 Gender-wise distribution of customers
select Gender,count(Customer_ID) as total_customers
from customers
group by Gender; 

--  Level 2: Revenue Analysis
-- 1) Find revenue by plan type (Monthly vs Yearly)
select Plan_Type, sum(Total_Revenue) as revenue
from subscriptions
group by Plan_Type
order by revenue desc
;

-- )2 Find top 5 plans by revenue
select Plan_Name,sum(Total_Revenue) as revenue 
from subscriptions
group by plan_name
order by revenue desc
limit 5 ;

-- )3 Find average revenue per customer

select  avg(Total_Customer_Revenue) 
as average_revenue_per_customer
from  (select Customer_ID, sum(Total_Revenue) as Total_Customer_Revenue
from 
subscriptions
group by Customer_ID) as t;

-- )4 Which state generated the highest revenue?
select c.state, sum(s.Total_Revenue) as total_revenues
from subscriptions s
join customers c
on c.Customer_ID = s.Customer_ID
group by c.state
order by total_revenues desc limit 1;
-- )5 Calculate total revenue from all subscriptions
 select  sum(Total_Revenue) as total_revenue 
from subscriptions;
 
--  Level 3: Churn Analysis
-- )1 Calculate overall churn percentage
select   count(case when Churn_Status =
 'Yes' then 1 end)*100.0/count(*) as churn_percentage
 from customers;
 
 
-- )2 Find state-wise churn percentage

 select State, sum(case when Churn_Status =
 'Yes' then 1 else 0 end)*100.0/count(*) as churn_percentage
 from customers
 group by State 
 order by churn_percentage desc ;
 
-- )3 Which 10 customers caused the most revenue loss due to churn?
select  s.Customer_ID, c.Name, c.Churn_Status,
sum(s.Total_Revenue) as Total_Revenues
from subscriptions s
join customers c on s.Customer_ID = c.Customer_ID
where c.Churn_Status = 'Yes'
group by Customer_ID , c.Name
order by Total_Revenues desc limit 10 ;

-- )4 Find top 5 states with highest revenue loss due to churn
select c.State, c.Churn_Status, 
sum(s.Total_Revenue) as Total_Revenues
from subscriptions s
join customers c on s.Customer_ID = c.Customer_ID
where c.Churn_Status = 'Yes'
group by c.State
order by Total_Revenues desc
limit 5;
-- )5Find average age of churned customers vs active customers
select Churn_Status, avg(age) as average_age
from customers
group by Churn_Status;
 
-- Level 4: Usage Analysis
-- )1 Calculate average monthly data usage per customer
select  Customer_ID,
 avg(Data_Used_GB) as avg_monthly_data_usage
 from usage_i
 group by Customer_ID;
-- )2 Which customer has the highest total data usage?
select Customer_ID, sum(Data_Used_GB) as total_data_usage 
from usage_i
group by customer_id 
order by total_data_usage desc
limit 1;
-- )3 Find top 10 customers with maximum complaints
select u.customer_id, c.Name, sum(u.Complaints_Registered) as
 total_complaints
from usage_i u
join customers c on u.customer_id = c.customer_id
group by u.customer_id , c.Name
order by total_complaints desc
limit 10;
-- )4 Find customers who paid late more than 3 times
select u.customer_id , c.Name, sum(u.Late_Payments)
 as total_Late_Payments
 from usage_i u
 join customers c
 on u.customer_id = c.customer_id 
 group by u.customer_id , c.name 
having total_Late_Payments >3;
 
 
-- )5 Find correlation: customers with complaints >2 ka churn rate kitna hai?
select sum(case when c.Churn_Status = 
'Yes' then 1 else 0 end)*100.00/count(*) as churn_rate_high_complaints
from customers c
join (select customer_id
from usage_i
group by customer_id 
having  sum(Complaints_Registered) > 2) u
 on u.customer_id = c.customer_id;

 

-- Level 5: Time Series Analysis
-- )1 Month-wise new customers joined
select  date_format(Join_Date, '%y-%m') AS MonthS, 
count(Customer_ID) as new_customers
from customers 
group by MonthS
order by MonthS;

-- )2 Month-wise churned customers count
select date_format(Join_Date, '%y-%m') AS MonthS, Churn_Status,
count(Customer_ID) as total_customers
from customers
where churn_status = 'Yes'
group by MonthS
order by total_customers ;




-- )3 Month-wise total revenue trend
select date_format(Start_Date, '%y-%m') as months , 
sum(Total_Revenue) as revenues
from subscriptions 
group by months
order by revenues desc
;

-- )4 Month-wise average usage trend
select date_format(Month_Year, '%y-%m') AS MONTHS , avg(Total_Minutes_Used) AS Average_Minutes_used 
from usage_i
group by MONTHS
ORDER BY MONTHS;
-- )5 Find the month which had maximum churn
select date_format(Churn_Date, '%y-%m') as months , count(Customer_ID) as churned_customers
from customers
where churn_status = 'Yes'
group by months
order by churned_customers desc limit 1
;

-- Level 6: Advanced Joins
-- )1 List customers with total revenue, total usage, and churn status
SELECT 
    c.Customer_ID,
    c.Name,
    c.Churn_Status,
    SUM(s.Total_Revenue) AS REVENUE,
    SUM(u.Total_Minutes_Used) AS usage_minutes
FROM
    customers c
        LEFT JOIN
    subscriptions s ON   s.Customer_ID = c.Customer_ID
        LEFT JOIN
    usage_i u ON  u.Customer_ID = c.Customer_ID
GROUP BY c.Customer_ID , c.Churn_Status,c.Name
ORDER BY REVENUE DESC;
-- )2 Find customers who churned but generated revenue > 10,000
SELECT 
    c.Customer_ID,
    c.Name,
    c.Churn_Status,
    SUM(s.Total_Revenue) AS total_Revenue
FROM
    customers c
        JOIN
    subscriptions s ON c.Customer_ID = s.Customer_ID
WHERE
    Churn_Status = 'Yes'
GROUP BY c.Customer_ID , c.Name
HAVING total_Revenue > 10000;

-- )3 Find customers who never raised a complaint and are active
SELECT 
    c.Customer_ID,
    c.Name,
    u.Complaints_Registered,
    c.Churn_Status
FROM
    customers c
       left JOIN
    usage_i u ON c.Customer_ID = u.Customer_ID
WHERE
    c.Churn_Status = 'No'
GROUP BY c.Customer_ID , c.Name ,u.Complaints_Registered
having sum(u.Complaints_Registered) = 0 ;


-- )4 Find top 5 customers by lifetime revenue
select s.Customer_ID , c.Name, sum(s.Total_Revenue) as Total_Revenues
from customers c
join subscriptions s
on s.Customer_ID = c.Customer_ID
group by s.Customer_ID,c.Name
order by Total_Revenues desc
limit 5;


-- )5 Identify “High Risk Customers” = complaints >2 AND data usage < 10 GB last month
 
 

  SELECT 
    c.Customer_ID,
    c.Name,
    u.Month_Year,
    u.Data_Used_GB,
    total.total_complaints
FROM customers c
JOIN usage_I u 
    ON c.Customer_ID = u.Customer_ID
JOIN (
    SELECT Customer_ID,
           SUM(Complaints_Registered) AS total_complaints
    FROM usage_I
    GROUP BY Customer_ID
) total 
    ON u.Customer_ID = total.Customer_ID
WHERE total.total_complaints > 2
  AND u.Data_Used_GB < 10
  AND u.Month_Year = (SELECT MAX(Month_Year) FROM usage_I);

use customer_churn_analysis;













