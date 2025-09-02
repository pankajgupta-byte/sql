-- Databricks notebook source
use default 


-- COMMAND ----------

alter table customer_flight_activity rename to activity;
alter table customer_loyalty_history rename to hist;

-- COMMAND ----------

alter table activity rename column `Loyalty Number` to loyalty_number;
alter table activity rename column `Total Flights` to total_flights;
alter table activity rename column `Points Accumulated` to points_accumulated;
alter table activity rename column `Points Redeemed` to points_redemmed;
alter table activity rename column `Dollar Cost Points Redeemed` to dollar_cost_points_redeemed;

-- COMMAND ----------

alter table activity rename column `Year` to yr;
alter table activity rename column `Month` to mon;
alter table activity rename column `Distance` to dist;

-- COMMAND ----------

select * from activity limit 5

-- COMMAND ----------

alter table hist rename column`Enrollment Year` to enrollment_yr;
alter table hist rename column `Postal Code` to postal_code;
alter table hist rename column `Enrollment Month` to enrollment_mon;
alter table hist rename column `Cancellation Year` to cancellation_yr;
alter table hist rename column `Loyalty Card` to loyalty_card;
alter table hist rename column `Cancellation Month` to cancellation_mon;
alter table hist rename column `Marital Status` to marital_status;
alter table hist rename column `Enrollment Type` to enrollment_type;

-- COMMAND ----------

select * from hist limit 5;

-- COMMAND ----------

select count(cancellation_yr) from hist where cancellation_yr is not null

-- COMMAND ----------

--- 1.	Select all fields for the first 10 rows in the LoyaltyHistory table.
select * from hist limit 10

-- COMMAND ----------

---2.	Find the Loyalty Number and Total Flights for all activities that occurred in the Year 2017.
select loyalty_number, total_flights from activity where yr = 2017;

-- COMMAND ----------

--- 3.	Retrieve the Loyalty Number, City, and Province for customers residing in 'California'.
select loyalty_number, city, province from hist where province ='California'

-- COMMAND ----------

---4.	List the unique values for the Education field from the LoyaltyHistory table.
select distinct education from hist

-- COMMAND ----------

--- 5.	Find all flight activities where Points Redeemed is greater than 0. Order the results by Points Redeemed in descending order
select * from activity where points_redemmed >0 order by points_redemmed desc

-- COMMAND ----------

--- 6.	Select the Loyalty Number, Gender, and Marital Status for all female ('F') customers who are 'Single'.
select loyalty_number, gender, marital_status from hist where gender = 'Female' and marital_status='Single'

-- COMMAND ----------

--- 7.	Find the Loyalty Number and Salary for customers whose salary is between 50,000 and 60,000 (inclusive).
select loyalty_number, salary from hist where salary between  50000 and 60000

-- COMMAND ----------

---8.	List all distinct Country names from the LoyaltyHistory table.
select distinct country from hist 

-- COMMAND ----------

select * from hist limit 1

-- COMMAND ----------

---9.	Find all flight activities where the Distance traveled is exactly 0.
select * from activity where dist =0

-- COMMAND ----------

---10.	Retrieve the Loyalty Number and Loyalty Card status for customers whose Loyalty Card is either 'Nova' or 'Star'.
select loyalty_number, loyalty_card from hist where loyalty_card = 'Nova' or loyalty_card = 'Star'

-- COMMAND ----------

---11.	Calculate the total number of records (rows) in the FlightActivity table.
select count(*) from activity

-- COMMAND ----------

---12.	Find the total Points Accumulated across all customers for all time.
select round(sum(points_accumulated)/100000) as `In Millions` from activity

-- COMMAND ----------

---13.	Determine the average Salary of all customers.
select avg(salary) from hist 

-- COMMAND ----------

---14.	Count how many customers belong to each Marital Status category.
select distinct marital_status from hist;
select marital_status,count(*)
     from hist 
     group by marital_status


-- COMMAND ----------

---15.	Find the maximum CLV (Customer Lifetime Value) and the minimum CLV.
select max(clv), min(clv) from hist;
(select max(clv)from hist) union (select min(clv) from hist)

-- COMMAND ----------

---16.	Calculate the average Total Flights for each Year in the FlightActivity table.
select yr, round(sum(total_flights),2) from activity group by yr

-- COMMAND ----------

---17.	Group customers by Education level and calculate the average Salary for each level.
select education, round(avg(salary),2) from hist group by education


-- COMMAND ----------

---18.	Find the total number of flights (Total Flights) booked by customers residing in each City.
select city, sum(total_flights) from activity a inner join hist h on a.loyalty_number = h.loyalty_number 
group by city order by sum(total_flights) desc

-- COMMAND ----------

---19.	Count the number of customers whose Loyalty Card is 'Aurora'.
select count(*) from hist where loyalty_card ='Aurora'

-- COMMAND ----------

---20.	Determine the month with the highest total Distance traveled.
select mon, sum(dist) from activity group by mon order by sum(dist) limit  1

-- COMMAND ----------

---21.	Find all Country names that have more than 50 customers.
-- there is only onbe country 

-- COMMAND ----------

---22.	Calculate the total Dollar Cost Points Redeemed for the year 2017.
select sum(dollar_cost_points_redeemed) from activity where yr = 2017

-- COMMAND ----------

---23.	Group the FlightActivity data by Loyalty Number and find the total Points Redeemed for each customer, but only include customers who redeemed more than 1,000 points in total.
select loyalty_number, sum(points_redemmed) from activity group by loyalty_number having sum(points_redemmed) >1000

-- COMMAND ----------

---24.	Find the average Distance traveled for flight activities where Total Flights is greater than 5.
select avg(dist) from activity where total_flights>5

-- COMMAND ----------

---25.	For each Loyalty Card status, find the average CLV.
select loyalty_card, round(avg(clv),2) from hist group by loyalty_card

-- COMMAND ----------

----Join LoyaltyHistory and FlightActivity tables to list the City, Gender, and Total Flights for the month of January (Month = 1).
select h.city, h.gender, a.total_flights from activity a join hist h on a.loyalty_number = h.loyalty_number where mon = 1 


-- COMMAND ----------

---27. List the Loyalty Number, CLV, and the Total Flights for the year 2017. Include all customers from LoyaltyHistory, even those with no flight activity in 2017 (show Total Flights as NULL or 0).
select a.loyalty_number, h.clv, a.total_flights from activity a left join hist h on a.loyalty_number= h.loyalty_number where yr = 2017

-- COMMAND ----------

---28.	Find the Loyalty Number, City, and the total Points Accumulated across all time for customers who enrolled in the year 2018
select a.loyalty_number, h.city, sum(a.points_accumulated) from activity a inner join hist h on a.loyalty_number = h.loyalty_number where enrollment_yr = 2018 group by a.loyalty_number, h.city

-- COMMAND ----------

--- 29.Find the Loyalty Number and Enrollment Year of the customer who has the highest CLV.
select loyalty_number, enrollment_yr, clv from hist where clv = (select max(clv) from hist)

-- COMMAND ----------

---30.	Find the average salary of customers who have a Loyalty Card status of 'Star' or better (Star, Nova, Aurora) and whose primary residence is in Canada.
select loyalty_card, round(avg(salary)) from hist group by loyalty_card

-- COMMAND ----------

--- 31.	Calculate the percentage of total Distance contributed by flights in each Year.
select yr, 
sum(dist),
  ROUND(100.0 * SUM(dist) / SUM(SUM(dist)) OVER (), 2) AS percent_of_total
from activity group by yr;
select yr,
round(100* sum(dist)/sum(sum(dist)) over(),2) as f 
from activity group by yr

-- COMMAND ----------

---32.	Find the Loyalty Number of customers who have redeemed points (Points Redeemed > 0) but have never accumulated any points (Points Accumulated = 0) in the same month.
select loyalty_number,points_redemmed, points_accumulated from activity where points_redemmed > 0 and points_accumulated = 0 

-- COMMAND ----------

--- 33. calculate the total flights for each customer, and then find the average of that total flight count across all customers.
select loyalty_number, sum(total_flights), round(avg(total_flights),1) from activity group by loyalty_number order by sum(total_flights) desc

-- COMMAND ----------

---34.	Find the Loyalty Number of customers who have booked flights in every month (Month 1 through 12).
select loyalty_number from activity where total_flights > 0 group by loyalty_number having count(distinct mon) = 12 

-- COMMAND ----------

---35.	Find the Gender and the total Distance traveled for only the top 10% of customers based on Salary. 
with ranked_customers as (select *, ntile(10) over (order by salary desc) as decile from hist)
select rc.gender, sum(a.dist) from activity a join ranked_customers rc on a.loyalty_number = rc.loyalty_number 
where decile = 1
group by rc.gender


-- COMMAND ----------

---36.	Identify customers whose total Points Accumulated is greater than their total Distance traveled (across all time).
select loyalty_number from activity group by loyalty_number having sum(points_accumulated) > sum(dist) 

-- COMMAND ----------

---Find pairs of customers who share the same City and Education level. List their Loyalty Numbers.
select  h.loyalty_number, g.loyalty_number from hist h join hist g on h.loyalty_number=g.loyalty_number where h.city=g.city and h.Education=g.Education and  h.loyalty_number <> g.loyalty_number

-- COMMAND ----------

---39.	For each Province, find the highest Salary recorded.
select province, max(salary) 
from hist 
group by province 

-- COMMAND ----------

--- ascending , top lowest salaries --
select * from (
select province, salary,
rank() over(partition by province order by salary) as df 
from hist
where salary is not null)
where df <3

-- COMMAND ----------

---40.	Find the Loyalty Number of customers who enrolled in 2018 but cancelled their membership in a different year.
select loyalty_number from hist where enrollment_yr =2018 and cancellation_yr <> 2018

-- COMMAND ----------

---For each Year, rank the customers based on their Points Accumulated in that year. Display the Loyalty Number, Year, Points Accumulated, and the rank.
select h.loyalty_number, a.yr, sum(a.points_accumulated),
rank() over(partition by a.yr order by sum(a.points_accumulated) desc) as rank
from activity a join hist h on a.loyalty_number = h.loyalty_number
group by h.loyalty_number, a.yr 

-- COMMAND ----------

-- For each Year, rank the customers based on their Points Accumulated in that year.
SELECT
  loyalty_number,
  yr,
  points_accumulated,
  RANK() OVER (
    PARTITION BY yr
    ORDER BY points_accumulated DESC
  ) AS rank
FROM (
  SELECT
    h.loyalty_number,
    a.yr,
    SUM(a.points_accumulated) AS points_accumulated
  FROM
    activity a
    JOIN hist h ON a.loyalty_number = h.loyalty_number
  GROUP BY
    h.loyalty_number,
    a.yr
)

-- COMMAND ----------

---For each customer, find the difference between the Total Flights in the current month and the Total Flights in the previous month. Order by Year and Month.
select loyalty_number, yr, mon, total_flights, 
  lag(total_flights) over (
  partition by loyalty_number order by yr, mon) as previus_month,
abs(total_flights-previus_month) as diff
from activity 
order by loyalty_number, yr, mon

-- COMMAND ----------

--- 43. For each Loyalty Card status ('Star', 'Nova', 'Aurora'), find the average total Distance traveled by customers with that status
select h.loyalty_card, count(distinct h.loyalty_number), sum(a.dist), round(sum(dist)/(count(distinct h.loyalty_number))) from activity a join hist h on a.loyalty_number = h.loyalty_number group by h.loyalty_card 

-- COMMAND ----------

--- 44. Find the count of customers who enrolled in 2017 (Enrollment Year = 2017) and have not cancelled their membership yet (i.e., Cancellation Year is NULL or empty).
select count(*) from hist
 where enrollment_yr = 2017 and 
cancellation_yr is null

-- COMMAND ----------

---45.	Identify the Loyalty Number and CLV of customers whose total Dollar Cost Points Redeemed is greater than 5% of their CLV.
with grp as (
  select loyalty_number, sum(dollar_cost_points_redeemed) as tot_points
  from activity
  group by loyalty_number
) 
select h.loyalty_number, g.tot_points, h.clv  from hist h join grp g on h.loyalty_number = g.loyalty_number
where tot_points > 0.05*clv


-- COMMAND ----------

---46.	Find the average CLV for customers in each Country who have an average Distance traveled of more than 500 km per flight activity month.
with df as ( 
  select loyalty_number, yr, mon, try_divide(sum(dist),sum(total_flights)) as avg_distance
  from activity 
  group by loyalty_number, yr, mon
  having avg_distance > 500
)
select d.loyalty_number, h.clv from hist h join df d on h.loyalty_number= d.loyalty_number 
--- avg is for whole month

-- COMMAND ----------

--- for at least one month 
WITH df AS ( 
  SELECT 
    loyalty_number, 
    yr, 
    mon, 
    TRY_DIVIDE(SUM(dist), SUM(total_flights)) AS avg_distance
  FROM activity 
  GROUP BY loyalty_number, yr, mon
  HAVING avg_distance > 2498
),
qualified_customers AS (
  SELECT DISTINCT loyalty_number
  FROM df
)
SELECT 
  h.province,
  AVG(h.clv) AS avg_clv
FROM qualified_customers qc
JOIN hist h ON qc.loyalty_number = h.loyalty_number
GROUP BY h.province
ORDER BY avg_clv DESC;

-- COMMAND ----------

---47.	(Window Function - Cumulative Sum) Calculate the running total of Distance traveled for each customer, ordered by Year and Month.
select loyalty_number, yr, mon,
sum(dist) over (partition by loyalty_number order by yr, mon rows between unbounded preceding and current row) as cum_dist
from activity
order by loyalty_number, yr, mon

-- COMMAND ----------

select loyalty_number, yr, mon,dist,
sum(dist) over (partition by loyalty_number order by yr, mon rows between unbounded preceding and current row) as cum_dist
from activity
order by loyalty_number, yr, mon

-- COMMAND ----------

---48.	Find any Loyalty Number that appears in the FlightActivity table but not in the LoyaltyHistory table (or vice versa).
select loyalty_number from activity where loyalty_number not in (select loyalty_number from hist)

-- COMMAND ----------

select loyalty_number from hist where loyalty_number not in (select loyalty_number from activity)

-- COMMAND ----------

---49.	 Calculate the total Points Accumulated by customers who had the Enrollment Type '2018 Promotion' versus those with 'Standard' enrollment.

with df as (
  select loyalty_number, sum(points_accumulated) as points
  from activity
  group by loyalty_number
)
select h.enrollment_type, sum(d.points) from hist h join df d on h.loyalty_number = d.loyalty_number
where h.enrollment_type in ('Standard','2018 Promotion')
group by h.enrollment_type


-- COMMAND ----------

select * from activity limit 1

-- COMMAND ----------

---50.	Find the top 5 cities based on the average Total Flights per customer residing in that city.
with flights as (
  select loyalty_number, sum(total_flights) as sum_f
  from activity
  group by loyalty_number
)
select h.city, avg(f.sum_f)
from hist h join flights f on h.loyalty_number = f.loyalty_number 
group by h.city
order by avg(f.sum_f) desc 
limit 5

-- COMMAND ----------

---51.	Calculate the total Distance traveled for each Gender and Education level.
select gender, education, sum(dist) from activity a join hist h on a.loyalty_number = h.loyalty_number
group by gender, education
order by gender, Education

-- COMMAND ----------

with df as (
  select loyalty_number, sum(dist) as dst
  from activity
  group by loyalty_number
)
select h.gender, h.education, sum(d.dst) from hist h join df d on h.loyalty_number = d.loyalty_number
group by h.gender, h.education
order by gender, education

-- COMMAND ----------

---52.	Find the average Salary for customers whose Enrollment Year is the same as the year they first booked a flight (Total Flights > 0).
with df as (
  select loyalty_number, min(yr) as yer, sum(total_flights) as tot_flights
  from activity
  where total_flights >0
  group by loyalty_number
)
select avg(h.salary)
from hist h join df d on h.loyalty_number = d.loyalty_number
where d.yer = h.enrollment_yr

-- COMMAND ----------

---53.	Identify Loyalty Numbers that have Total Flights booked in at least 3 different years.
select loyalty_number, count(distinct yr) from activity
group by loyalty_number
having count(distinct yr) >=2

-- COMMAND ----------

---54.	Find the Country and Province with the highest average CLV per customer, where the average Total Flights per customer is also greater than 50.

with df as (
  select loyalty_number, avg(total_flights) as tot
  from activity 
  group by loyalty_number
),
cust as (
select h.province, h.city, h.clv, d.tot
from hist h join df d on h.loyalty_number = d.loyalty_number
where d.tot > 50
)
select province, city, avg(clv) from cust  
group by province, city 

-- COMMAND ----------

---55.	For each Loyalty Card status (Star, Nova, Aurora), calculate the percentage of total Points Accumulated contributed by that status.
with df as (
  select loyalty_number, sum(points_accumulated) as pa
  from activity
  group by loyalty_number
),
grouped as (
select h.loyalty_card, sum(d.pa) as total_pa
from hist h join df d on h.loyalty_number = d.loyalty_number
group by h.loyalty_card
),
total as (
  select sum(total_pa) as grand_total 
  from grouped 
)
select g.loyalty_card, 
round(total_pa/grand_total*100,2) as pct_cont
from grouped g
cross join total t

-- COMMAND ----------

select loyalty_card, 
round(sum(points_accumulated) *100 /sum(sum(points_accumulated)) over(),2) as pct_cnt
from activity a join hist h on a.loyalty_number = h.loyalty_number
group by loyalty_card

-- COMMAND ----------

---57.	Calculate the total Dollar Cost Points Redeemed per Marital Status for customers who enrolled in 2018.
select marital_status, sum(dollar_cost_points_redeemed),
round(sum(dollar_cost_points_redeemed)*100/sum(sum(dollar_cost_points_redeemed)) over(),2) as in_pct
from activity a join hist h on a.loyalty_number=h.loyalty_number
where h.enrollment_yr = 2018
group by marital_status

-- COMMAND ----------

---58.	List the top 3 City and Gender combinations based on the total Distance traveled.
select h.city,
      sum(case when h.gender = 'Male' then a.dist else 0 end) as Male,
      sum(case when h.gender = 'Female' then a.dist else 0 end) as Female,
round(  
      sum(case when h.gender = 'Male' then a.dist else 0 end)*100.0/
      sum(sum(case when h.gender = 'Male' then a.dist else 0 end))over(),
      2) 
      as male_pct
from activity a join hist h on a.loyalty_number=h.loyalty_number
group by h.city
order  by h.city

-- COMMAND ----------

with df as (
select h.city,
      sum(case when h.gender = 'Male' then a.dist else 0 end) as Male,
      sum(case when h.gender = 'Female' then a.dist else 0 end) as Female
from activity a join hist h on a.loyalty_number=h.loyalty_number
group by h.city
order  by h.city)
select city, Male, Female,
round(Male/(Male+Female) *100,2) as male_pct,
round(Female/(Male+Female)*100,2) as fem_pct,
(Male+ Female) as tot_dist
from df
order by tot_dist desc 
limit 3

-- COMMAND ----------

---59.	Find the average CLV for customers who have never redeemed any points (Points Redeemed is 0 for all their flights).
select avg(clv) from activity a join hist h on a.loyalty_number=h.loyalty_number
where points_redemmed = 0 or points_redemmed is null

-- COMMAND ----------

---60.	For each Loyalty Number, find the number of months in which they flew at least 1,000 km 
select loyalty_number, count(distinct yr * 100 + mon) as tot_mon
from activity
where dist > 1000
group by loyalty_number

-- COMMAND ----------

--- 61.  For each Year, rank customers based on their total Distance traveled, from highest to lowest.
select loyalty_number, 
       yr,
       rank() over(partition by yr order by sum(dist) desc) as rank_dist
       from activity
       group by loyalty_number, yr
       order by loyalty_number,rank_dist


-- COMMAND ----------

---62. Use DENSE_RANK() to rank customers based on their CLV, partitioning the data by Education level.
select loyalty_number, education,
dense_rank() over(partition by education order by sum(clv) desc) as ranked_clv
from hist
group by loyalty_number, education

-- COMMAND ----------

---63.	 Divide all customers into 4 equal groups (NTILE(4)) based on their Salary, and find the average CLV for each group.
select g.t as grps, round(avg(g.clv)) as clv, round(avg(g.salary)) as salary
 from (select salary, clv, ntile(4) over(order by salary) as t 
from hist
where salary is not null
) g
group by g.t

-- COMMAND ----------

---64. For each Loyalty Number, find the Distance traveled in the next month. Show Loyalty Number, Year, Month, Distance, and the next_month_distance.
select loyalty_number, yr, mon, sum(dist),
lead(sum(dist)) over(partition by loyalty_number order by yr, mon) as next_mon_dist
from activity
group by loyalty_number, yr, mon

-- COMMAND ----------

select loyalty_number, yr, mon, 
sum(dist),
sum(sum(dist)) over(
  partition by loyalty_number 
  order by yr, mon 
  ROWS BETWEEN UNBOUNDED PRECEDING AND 
  CURRENT ROW
 ) as cum_dist
from activity
group by loyalty_number, yr, mon
--- either use double sum or CTE epression 

-- COMMAND ----------

---65.For each Loyalty Number, find the difference in Total Flights between the current month and the previous month. Show Loyalty Number, Year, Month, Total Flights, and the difference.
select loyalty_number, yr, mon, sum(total_flights), 
abs(sum(total_flights) - lag(sum(total_flights)) over( partition by loyalty_number order by yr, mon)) as diff
from activity
group by loyalty_number, yr, mon 

-- COMMAND ----------

---66. Calculate the running total of Points Accumulated for each Loyalty Number, ordered by Year and Month.
select loyalty_number,yr, mon, 
sum(points_accumulated) over (partition by loyalty_number order by yr, mon rows between unbounded preceding and current row) as cum_points
from activity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

---67. Calculate the running average of Distance traveled for each Loyalty Number, ordered by Year and Month.
select loyalty_number, yr, mon, 
round(avg(dist) over(partition by loyalty_number order by yr, mon rows between unbounded preceding and current row),2) as avg_dist
from activity

-- COMMAND ----------

--- 68.	For each Loyalty Number, calculate the average Distance traveled over a 3-month rolling window, ordered by Year and Month.
select loyalty_number , yr, mon, 
round(avg(dist) over(partition by loyalty_number order by yr, mon rows between 1 preceding and 1 following)) as 3_mon_avg
from activity

-- COMMAND ----------

---69.	Find the Loyalty Number of the top 3 customers in each Province based on their CLV.
select * from (
select province, loyalty_number, sum(clv),
rank() over(partition by province order by sum(clv)) as rank_ed
from hist 
group by province, loyalty_number)
where rank_ed <=3

-- COMMAND ----------

-- 70. For each Loyalty Number, find the Distance traveled during their very first flight and their very last flight
with df as (
  select loyalty_number, yr, mon, 
  sum(dist) as dst, 
  sum(total_flights) as t_fl
  from activity
  group by loyalty_number , yr, mon
),
ranked as (
  select * , 
  row_number() over(
    partition by loyalty_number order by yr, mon) as first_rank,
  row_number() over(
    partition by loyalty_number order by yr desc, mon desc
  ) as last_rank 
from df 
where dst is not null and t_fl is not null and dst >0 and t_fl>0
),
tp as (
select loyalty_number, yr,mon,
case when first_rank = 1 then dst end as first_flight_dst, 
case when last_rank = 1 then dst end as last_flight_dst 
from ranked
)
select loyalty_number, sum(first_flight_dst),
sum(last_flight_dst) 
from tp 
where first_flight_dst is not null or last_flight_dst is not null
group by loyalty_number

-- COMMAND ----------

------optimized version
WITH df AS (
  SELECT 
    loyalty_number, 
    yr, 
    mon, 
    SUM(dist) AS dst, 
    SUM(total_flights) AS t_fl
  FROM activity
  WHERE dist IS NOT NULL AND total_flights IS NOT NULL AND dist > 0 AND total_flights > 0
  GROUP BY loyalty_number, yr, mon
),
ranked AS (
  SELECT 
    loyalty_number, 
    yr, 
    mon, 
    dst,
    ROW_NUMBER() OVER (PARTITION BY loyalty_number ORDER BY yr, mon) AS first_rank,
    ROW_NUMBER() OVER (PARTITION BY loyalty_number ORDER BY yr DESC, mon DESC) AS last_rank
  FROM df
)
SELECT 
  loyalty_number,
  MAX(CASE WHEN first_rank = 1 THEN dst END) AS first_flight_dst,
  MAX(CASE WHEN last_rank = 1 THEN dst END) AS last_flight_dst
FROM ranked
GROUP BY loyalty_number;

-- COMMAND ----------

---71.	For each Year, find the difference in Points Accumulated between the highest-earning month and the lowest-earning month for each customer.

with df as (
select loyalty_number, yr, mon, 
sum(points_accumulated) as points
from activity
where points_accumulated is not null and points_accumulated >0
group by loyalty_number, yr, mon
),
ranked as (
  select loyalty_number,yr,
  max(points) as max_point,
  min(points) as min_point
  from df
  group by loyalty_number, yr
)
select loyalty_number, 
 max(case when yr = 2017 then abs(max_point - min_point) end) as diff_2017,
 max(case when yr = 2018 then abs(max_point - min_point) end) as diff_2018
 from ranked
group by loyalty_number


-- COMMAND ----------

---72.	Use a window function to find customers whose Total Flights in a given month is greater than the average Total Flights for that same Month across all years.

with df as (
  select loyalty_number, mon, total_flights,
  avg(total_flights) over(partition by mon) as avg_flights
  from activity
)
select distinct mon, count(loyalty_number)
from df 
where total_flights>avg_flights
group by mon 

-- COMMAND ----------

select loyalty_number, mon, total_flights,
  avg(total_flights) over(partition by mon) as avg_flights
  from activity

-- COMMAND ----------

---73.	Find the Loyalty Number of customers who have a total Points Redeemed that falls within the top 20% of all customers.
select loyalty_number, points_redemmed from 
(select loyalty_number, points_redemmed,
ntile(5) over(order by points_redemmed desc) as decile
from activity 
) t
where decile = 1 

-- COMMAND ----------

----74. For each customer, what percentage of their total Points Accumulated in a given Year did a single month's activity account for?
--- can also be done by CTE's
select loyalty_number, yr, mon,
round(try_divide(points,tot)*100,2) as pct
from
(select loyalty_number, yr, mon,points_accumulated as points,
sum(Points_accumulated) over(partition by loyalty_number) as tot
from activity) t 
order by loyalty_number, yr, mon

-- COMMAND ----------

--- 75.	Rank the Education levels based on the average Salary, but only for customers who have a Loyalty Card status of 'Aurora'.
select education, round(avg(salary)) as salry,
rank() over(order by avg(salary) desc) as ed_rank
from hist
where loyalty_card = 'Aurora'
group by education

-- COMMAND ----------

---76. Use a CTE to calculate the total Distance for each customer, and then find the names of customers (Gender, Education) whose total Distance is above the overall average.
with df as (
  select loyalty_number, sum(dist) as dst
  from activity 
  group by loyalty_number
)
select h.loyalty_number, h.gender, h.education
from hist h join df d on h.loyalty_number = d.loyalty_number
where dst > (select avg(dst) from df)

-- COMMAND ----------

select loyalty_number, sum(dist) as dst,
  sum(sum(dist)) as avg_dst
  from activity 
  group by loyalty_number


-- COMMAND ----------

---77. Find the Loyalty Number of customers who have a CLV greater than the average CLV of customers who enrolled in the Enrollment Year 2018.
select loyalty_number, clv
from hist
where enrollment_yr = 2018 and 
clv > (select avg(clv) from hist where enrollment_yr = 2018)


-- COMMAND ----------

--- 78.	Identify Loyalty Numbers that have flown more than 5,000 km in a single month but have a CLV of less than $1,000.
select a.loyalty_number, a.yr, a.mon, sum(a.dist),sum(h.clv)
from activity a join hist h on a.loyalty_number = h.loyalty_number
where a.dist > 5000 and h.clv <1000
group by a.loyalty_number, a.yr, a.mon
order by loyalty_number

-- COMMAND ----------

---79.	Find the Loyalty Numbers of customers who have redeemed points in a month where Points Redeemed was greater than Points Accumulated.
with df as (
  select loyalty_number, yr, mon, 
  points_accumulated as pa,
  points_redemmed as pr
  from activity
  order by loyalty_number, yr, mon
)
select loyalty_number
from df 
where pr>pa

-- COMMAND ----------

----80. Find the average Salary of customers who have a Loyalty Card status of 'Aurora' and have accumulated more than 10,000 points in total.
with df as (
  select loyalty_number, sum(points_accumulated) as pa
  from activity
  group by loyalty_number
  having sum(points_accumulated) > 10000
)
  select  avg(h.salary) as sal
  from hist h 
  join df d on h.loyalty_number = d.loyalty_number
  where h.loyalty_card = 'Aurora'


-- COMMAND ----------

---81.	Scenario: "Frequent Flier" * Find the Loyalty Number and Marital Status of customers who rank in the top 10% for both Total Flights and Distance traveled (across all time).
  with df as (
  select loyalty_number, sum(total_flights),
  ntile(10) over(order by sum(total_flights) desc) as decile
  from activity
  group by loyalty_number
),
dg as (
select loyalty_number, sum(dist),
ntile(10) over(order by sum(dist) desc) as dfile
from activity
group by loyalty_number
)
(select loyalty_number from df
where decile = 1)
intersect
(select loyalty_number from dg
where dfile = 1)

-- COMMAND ----------

---82.	Scenario: "High Value, Low Engagement" * Find the Loyalty Number of customers with a CLV in the top 10% of all CLVs, but who have booked fewer than 5 flights in the last 2 years of data.
with df as (
  select loyalty_number, sum(total_flights) as tf
  from activity
  group by loyalty_number
),
dg as (
  select loyalty_number, clv,
  ntile(10) over(order by clv desc) as decile 
  from hist
)
(select loyalty_number from df 
where tf < 5)
intersect
(select loyalty_number from dg
where decile = 1)

-- COMMAND ----------

---83.	Scenario: "Promotion Effectiveness" * Compare the average CLV of customers who were part of the '2018 Promotion' (Enrollment Type) against those with a 'Standard' enrollment. Is there a significant difference?
select enrollment_type, round(avg(clv))
from hist 
group by enrollment_type

-- COMMAND ----------

---84.	Scenario: "Geographic Performance" * For each Country, calculate the total Points Accumulated and the total Dollar Cost Points Redeemed. Find the top 3 countries with the highest ratio of Points Accumulated to Points Redeemed.
select province, sum(points_accumulated) as pa, sum(points_redemmed) as pr,
round(sum(points_redemmed)*100/sum(points_accumulated),5) as pct_used
from activity a join hist h on a.loyalty_number = h.loyalty_number
group by Province
order by pct_used desc

-- COMMAND ----------

---85.	Find the Loyalty Number and Year for all customers whose Points Accumulated in that year exceeded the total Points Redeemed by more than a 2x factor.
select loyalty_number, yr, sum(points_accumulated) as pa, sum(points_redemmed) as pr
from activity
group by loyalty_number, yr
having pa > pr*3

-- COMMAND ----------

--- 86.	(Complex Join) Find the average Salary for each Gender and Education combination, but only for customers who have redeemed points in at least two different years.
select gender, education, round(avg(salary)) as sal
from hist h join activity a on h.loyalty_number = a.loyalty_number
group by gender, education
having count(distinct yr) > 1

-- COMMAND ----------

---88.	Identify customers who booked flights in every month of 2017 and 2018.
select count(distinct loyalty_number) from (
select loyalty_number, yr, mon
from activity 
where total_flights > 0 and total_flights is not null
group by loyalty_number, yr, mon 
having count(distinct mon) = 12 and count(distinct yr) = 2
order by loyalty_number) y 

-- COMMAND ----------

WITH months_per_year AS (
  SELECT 
    loyalty_number,
    yr,
    COUNT(DISTINCT mon) AS active_months
  FROM activity
  WHERE total_flights > 0
  GROUP BY loyalty_number, yr
),
qualified_customers AS (
  SELECT loyalty_number
  FROM months_per_year
  WHERE (yr = 2017 OR yr = 2018) AND active_months = 12
  GROUP BY loyalty_number
  HAVING COUNT(DISTINCT yr) = 2
)
SELECT * 
FROM qualified_customers;

-- COMMAND ----------

----89.	Find the average Salary of customers who canceled their membership (Cancellation Year is not NULL).
select avg(salary) as sal 
from hist
where cancellation_yr  is not null

-- COMMAND ----------

----91.	Find Loyalty Numbers where the Total Flights is exactly the same in two consecutive months for the same customer.
with df as (
select loyalty_number, yr, mon, total_flights as tf,
lead(total_flights) over(partition by loyalty_number order by yr, mon) as ltf
from activity
)
select loyalty_number
from df 
where tf = ltf
group by loyalty_number

-- COMMAND ----------

---92.	CTE: Use a CTE to calculate the total Total Flights for each customer, and then use the result to find the Loyalty Number of customers whose total Total Flights is an odd number.
with df as (
  select loyalty_number, sum(total_flights) as tf 
  from activity
  group by loyalty_number
 )
 select loyalty_number from df 
 where tf % 2 = 1

-- COMMAND ----------

select loyalty_number, sum(total_flights) as tf 
  from activity
  group by loyalty_number
  having tf % 2 =1

-- COMMAND ----------

---93.	Analytical: For each customer, what was the average Distance traveled in their first three months of activity?
with df as (
  select loyalty_number, yr, mon, dist as dst,
  row_number() over(partition by loyalty_number order by yr, mon) as rn
  from activity 
  order by loyalty_number, yr,mon
)
select loyalty_number,
round(avg(dst)) as first_3_avg
from df 
where rn <=3
group by loyalty_number

-- COMMAND ----------

---94.	Find the Loyalty Number, Year, and Month of all months where a customer's Points Accumulated was the highest for that entire year.
select * from (
select loyalty_number, yr, mon, points_accumulated,
max(points_accumulated) over(partition by loyalty_number) as mp
from activity) t
where points_accumulated = t.mp
order by loyalty_number, yr

-- COMMAND ----------

with df as  (
select loyalty_number, yr, mon, points_accumulated,
max(points_accumulated) over(partition by loyalty_number) as mp
from activity
)
select loyalty_number, yr, max(mp)
from df 
group by loyalty_number, yr
order by loyalty_number,yr

-- COMMAND ----------

---95.	Calculate the total CLV for customers in each Postal Code but only show postal codes where the total CLV is greater than 50,000.
select postal_code, round(sum(clv))
from hist
group by postal_code
having sum(clv) > 50000

-- COMMAND ----------

select count(distinct postal_code) from hist

-- COMMAND ----------

---96.	Complex Condition: Find the Loyalty Numbers of customers whose enrollment was a promotion, and who have a CLV less than the average CLV of standard-enrolled customers.
with df as (
  select loyalty_number, enrollment_type, clv 
  from hist 
)
select loyalty_number
from df where enrollment_type = '2018 Promotion'and clv<(select avg(clv) from df where enrollment_type = 'Standard')

-- COMMAND ----------

---97.	Join Logic: Find the number of customers whose Loyalty Card status changed from one year to the next.
 --- hist doesnt have yr data, while joining it gives same values 

-- COMMAND ----------

---98.	Find the City that has the most customers, and the City that has the fewest customers.
with df as (
select city, count(distinct loyalty_number) as num
from hist 
group by City
order by count(distinct loyalty_number) desc
),
ranked as (
  select city, num,
row_number() over(order by num desc) as rn,
row_number() over(order by num asc) as tn
from df 
)
select city, num,rn, tn
from ranked 
where rn = 1 or tn = 1

-- COMMAND ----------

select  city, num_customers
from (
  select city, count(distinct loyalty_number) as num_customers
  from hist
  group by city
) as city_counts
qualify row_number() over(order by num_customers desc) = 1
  or row_number() over(order by num_customers asc) = 1

-- COMMAND ----------

---99.	Final Challenge: Ratio Analysis
---For each Loyalty Card status, calculate the ratio of Total Flights to Points Accumulated. Rank the statuses based on this ratio, from highest to lowest.
with df as (
select loyalty_number as ln, sum(total_flights) as tf, sum(points_accumulated) as pa
from activity
group by loyalty_number
), 
dg as (
  select ln, tf, pa, h.loyalty_card as ls 
  from hist h join df d on h.loyalty_number = d.ln
)
select ln, 
round(try_divide(tf,pa),5) as ratio
from dg
order by round(try_divide(tf,pa),5) desc

-- COMMAND ----------

---100.	Final Challenge: Predictive Query * Based on the data, identify Loyalty Numbers who have a lower Total Flights in the most recent Year of data compared to their average Total Flights from all previous years. These might be "at-risk" customers.
with df as (
  select loyalty_number, sum(total_flights) as p_tf
from activity
where yr = 2018  
group by loyalty_number
),
dg as (
select loyalty_number, sum(total_flights) as tf
from activity
where yr = 2017
group by loyalty_number
) 
select d.loyalty_number, d.p_tf, g.tf 
from df d join dg g on d.loyalty_number = g.loyalty_number
where d.p_tf > g.tf

-- COMMAND ----------

