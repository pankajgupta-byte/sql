-- Databricks notebook source
use default;
alter table order_products_prior rename to ord_prd;

-- COMMAND ----------

---1.	How many orders are there in total?
select count(*) from orders

-- COMMAND ----------

---2.	What is the name of the aisle with aisle_id 24?
select aisle from aisles where aisle_id = 24

-- COMMAND ----------

---3.	List the top 5 most frequently ordered products.
select p.product_id, min(p.product_name),
count(*) as cn
from ord_prd o inner join products p on o.product_id = p.product_id
group by p.product_id
order by count(*) desc
limit 5

-- COMMAND ----------

---4.	Find the total number of unique customers.
select count(distinct order_id) from orders

-- COMMAND ----------

---5.	What are the names of all the departments?
select distinct department from departments

-- COMMAND ----------

alter table departments rename to depts

-- COMMAND ----------

alter table depts set tblproperties ('delta.columnMapping.mode' = 'name');
alter table depts rename column department to dept

-- COMMAND ----------

alter table depts rename column department_id to dept_id

-- COMMAND ----------

---6.	Find the department_id for the 'produce' department.
select *  from depts where dept ='produce'

-- COMMAND ----------

---7.	List all products that have the word 'Organic' in their name.
select * from products where product_name like '%Organic%'

-- COMMAND ----------

---8.	Count the number of orders placed on each day of the week, with the day of week shown as 0-6.
select order_dow, count(*) from orders group by order_dow order by order_dow

-- COMMAND ----------

---9.	Find the number of products in each aisle.
select aisle_id, count(*) from products group by aisle_id order by count(*) desc

-- COMMAND ----------

---10.	What is the total number of products that were reordered?
select count(distinct product_id) from ord_prd where reordered = 1

-- COMMAND ----------

---never reordered
select count(*) from (select product_id, count(*) from ord_prd group by product_id having max(reordered) = 0)

-- COMMAND ----------

---11.	What is the average number of days between orders for a customer?
-- no data

-- COMMAND ----------

---12.	Find the names of products that were reordered more than 100 times.
with df as (
select product_id, count(*) as cn
from ord_prd
where reordered = 1
group by product_id
having count(*) > 100
)
select p.product_id, p.product_name, d.cn
from df d join products p on d.product_id = p.product_id
order by d.cn desc


-- COMMAND ----------

---13.	List the top 10 most popular aisles based on the number of products sold.
select order_id, product_id, count(*) from ord_prd group by order_id, product_id having count(*) > 1;

-- COMMAND ----------

with df as (select product_id, count(order_id) cn
            from ord_prd group by product_id
), 
  dg as (
  select p.aisle_id, sum(d.cn) as s_a
  from df d join products p on d.product_id = p.product_id
  group by p.aisle_id
  order by sum(d.cn) desc 
  limit 10
)
select aisle, d.s_a from aisles a 
join dg d on a.aisle_id = d.aisle_id
order by d.s_a desc

-- COMMAND ----------

---14.	For each department, calculate the total number of orders.
with df as (select product_id, count(distinct order_id) cn
            from ord_prd group by product_id
), 
  dg as (
  select p.department_id, sum(d.cn) as s_a
  from df d join products p on d.product_id = p.product_id
  group by p.department_id
)
select dept, d.s_a from depts dp 
join dg d on d.department_id = dp.dept_id
order by d.s_a desc

-- COMMAND ----------

-- 14. Unique orders per department (each order counted once per department)
WITH order_dept AS (
    SELECT DISTINCT 
        o.order_id, 
        p.department_id
    FROM ord_prd o
    JOIN products p ON o.product_id = p.product_id
)
SELECT 
    d.dept AS department,
    COUNT(*) AS total_orders
FROM order_dept od
JOIN depts d ON od.department_id = d.dept_id
GROUP BY d.dept
ORDER BY total_orders DESC;

-- COMMAND ----------

---15.	Find the names of all products that belong to the 'produce' department and 'fresh fruits' aisle.
select p.product_name
from products p join aisles a on p.aisle_id = a.aisle_id
join depts d on p.department_id = d.dept_id
where d.dept = 'produce' and a.aisle = 'fresh fruits'

-- COMMAND ----------

---16.	Calculate the number of orders placed on each day of the week, displaying the day of the week as a string (e.g., 'Sunday', 'Monday').
select case order_dow
when 0 then 'Sunday'
when 1 then 'Monday'
when 2 then 'Tuesday'
when 3 then 'Wednesday'
when 4 then 'Thursday'
when 5 then 'Friday'
when 6 then 'Saturday'
end as week_day,
count(*) as total
from orders 
group by order_dow
order by total desc

-- COMMAND ----------

---17.	Find the average order size (number of unique products per order).
select avg(cn) from 
(select order_id, count(product_id) as cn from ord_prd
group by order_id)

-- COMMAND ----------

---18.	For each user, find their very first order (order_number = 1) and the total number of products in that order.
with first_ord as (
  select user_id, min(order_id) as ord_id
  from orders 
  where order_number = 1
  group by user_id
)
select f.user_id, o.order_id, count(*) as no_of_products from
first_ord f join ord_prd o on f.ord_id = o.order_id
group by f.user_id, o.order_id

-- COMMAND ----------

---19.	What is the most popular hour for ordering in each department?
select order_hour_of_day, count(distinct order_id)
from orders
group by order_hour_of_day
order by count(distinct order_id) desc

-- COMMAND ----------

---20.	Calculate the percentage of orders placed on Sundays vs. all other days.
select  
round(sum(case when order_dow = 0 then 1 else 0 end) * 100 / count(*),2) as pct_sunday,
round(sum(case when order_dow != 0 then 1 else 0 end) * 100 /count(*), 2) as pct_non_sunday
from orders

-- COMMAND ----------

ALTER TABLE products SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);

ALTER TABLE products RENAME COLUMN department_id TO dept_id;

-- COMMAND ----------

--- 21.	Find the top 3 most popular products in each department.
with df as(
  select product_id, count(distinct order_id) as prd
  from ord_prd
  group by product_id
),
dg as (
  select p.dept_id as dep, 
  rank() over(partition by p.dept_id order by d.prd desc) as rnk,
  d.prd as pr
  from df d join products p on d.product_id  = p.product_id
),
ds as (
  select g.dep, g.rnk,g.pr, dt.dept as de
  from dg g join depts dt on g.dep = dt.dept_id
)
select dep,de, rnk, pr
from ds where rnk <= 3

-- COMMAND ----------

---22.	Identify customers who have ordered products from at least 5 different departments.
select o.user_id
from 
orders o join ord_prd op on o.order_id = op.order_id
join products p on op.product_id = p.product_id
group by o.user_id
having count(distinct p.dept_id) >=5


-- COMMAND ----------

----23.	Calculate the reorder rate for each product. (Reorder rate = number of times a product was reordered / total number of times it was ordered).
with df as (
  select product_id, 
  count(*) as total_order,
  sum(case when reordered = 1 then 1 else 0 end) as reorder
  from ord_prd
  group by product_id
)
select product_id,
round(cast(reorder as float)/ total_order,3) as reorder_rate
from df 

-- COMMAND ----------

---24.	Find the most popular hour of the day for placing orders on each day of the week.
select order_hour_of_day, count(*)  from orders group by order_hour_of_day order by count(*) desc limit 1

-- COMMAND ----------

---25.	List the products that were never reordered.
with df as (
  select product_id,
  count(*) as total_order,
  sum(case when reordered = 1 then 1 else 0 end) as reorder
  from ord_prd 
  group by product_id
)
select product_id,
round(cast(reorder as float) / total_order,3) as reorder_rate
from df 
where round(cast(reorder as float) / total_order,3) = 0

-- COMMAND ----------

---26.	Identify the top 5 customers who have the highest number of unique products in their orders.
select o.user_id, count(distinct op.product_id) as unq_products
from orders o join ord_prd op on o.order_id = op.order_id 
group by o.user_id 
order by count(distinct op.product_id) desc 
limit 5

-- COMMAND ----------

--- highest products per person
select o.user_id, o.order_id,
count(distinct op.product_id) as unq_products
from orders o join ord_prd op on o.order_id = op.order_id 
group by o.user_id, o.order_id
order by count(distinct op.product_id) desc 
limit 5


-- COMMAND ----------

---27.	Find the products that were most frequently added to the cart first (add_to_cart_order = 1).
select product_id, count(*) from ord_prd where add_to_cart_order = 1 group by product_id order by count(*) desc limit 10

-- COMMAND ----------

---28.	For each user, find the total number of orders they have placed.
select user_id, count(distinct order_id) from orders group by user_id 

-- COMMAND ----------

---29.	Find the top 5 products that are most likely to be reordered (reordered = 1).
with df as (
  select product_id, 
  count(*) as total_order,
  sum(case when reordered = 1 then 1 else 0 end) as reorder
  from ord_prd
  group by product_id
)
select product_id,
round(cast(reorder as float)/ total_order,3) as reorder_rate
from df 
order by reorder_rate desc 
limit 5

-- COMMAND ----------

---30.	List the products that have never been ordered on a Sunday.
with df as (
  select op.product_id as product
  from ord_prd op join orders o on op.order_id = o.order_id
  where o.order_dow = 0
),
dg as (
  select distinct product_id from products
) 
select product_id from dg where product_id not in (select product from df)

-- COMMAND ----------

with df as (
  select product_id, 
  count(*) as total_order,
  sum(case when reordered = 1 then 1 else 0 end) as reorder
  from ord_prd 
  group by product_id
),
depts as (
  select p.product_id, dt.dept_id, dt.dept
  from products p join depts dt on p.dept_id = dt.dept_id
)
select ds.dept_id, ds.dept,
round(cast(sum(reorder) as float) / sum(total_order), 3) as reorder_rate
from df d join depts ds on d.product_id = ds.product_id
group by ds.dept_id, ds.dept
order by reorder_rate desc

-- COMMAND ----------

---33.	Identify the top 10 "market basket" product pairs, i.e., pairs of products that are most frequently purchased together in the same order.
with df as (
  select a.product_id as product_1,
  b.product_id as product_2
  from ord_prd a join ord_prd b on a.order_id = b.order_id
  where a.product_id > b.product_id
)
 select product_1, product_2, count(*) from df
  group by product_1, product_2
  order by count(*) desc 
  limit 10


-- COMMAND ----------

---35.	For each department, find the product with the highest reorder rate that has been ordered at least 100 times.
with df as (
  select p.product_id, d.dept_id, min(d.dept) as dept,
  count(*) as total_orders,
  sum(case when op.reordered = 1 then 1 else 0 end) as total_reorders,
  round(cast(sum(case when op.reordered = 1 then 1 else 0 end) as float) / count(*),3) as reorder_rate
  from ord_prd op 
  join products p on op.product_id = p.product_id
  join depts d on p.dept_id = d.dept_id
  group by p.product_id, d.dept_id 
  having count(*) >=100
),
ranked as (
  select * ,
  rank() over(partition by dept_id order by reorder_rate desc) as rnk
  from df 
)
select dept, product_id, reorder_rate from ranked where rnk = 1 order by dept 

-- COMMAND ----------

---36.	Identify customers who exclusively buy organic products.
with non_orgainc as (
select o.user_id
from ord_prd op 
join products p on op.product_id = p.product_id 
join orders o on op.order_id = o.order_id
where p.product_name like '%Organic%'
),
all_users as (
  select distinct user_id from orders 
),
exclusive_buyers as (
  select user_id from all_users where user_id not in (select user_id from non_orgainc)
)
select count(*) from exclusive_buyers

-- COMMAND ----------

---37.	For each user, rank their ordered departments from most to least frequently used.
with df as (
select o.user_id, d.dept_id, min(d.dept) as dept, count(*) as order_count
from ord_prd op 
join orders o on op.order_id = o.order_id
join products p on op.product_id = p.product_id
join depts d on p.dept_id = d.dept_id
group by o.user_id, d.dept_id
),
ranked as (
  select *, 
  rank() over(partition by user_id order by order_count desc) as rnk
  from df 
)
select user_id, dept, order_count, rnk from ranked order by user_id, rnk


-- COMMAND ----------

---38.	Find the orders that contain products from both the 'beverages' and 'snacks' departments.
select count(*) from ((
select  order_id
from ord_prd op 
join products p on op.product_id = p.product_id
join depts d on p.dept_id = d.dept_id
where d.dept = 'beverages' 
)
intersect
(
select  order_id
from ord_prd op 
join products p on op.product_id = p.product_id
join depts d on p.dept_id = d.dept_id
where d.dept = 'snacks'
))

-- COMMAND ----------

---39.	Calculate the average days_since_prior_order for orders that contain a product from the 'dairy eggs' department.
select avg(days_since_prior_order) as avg_days from (
select distinct o.order_id, o.days_since_prior_order
from ord_prd op
join orders o on op.order_id =  o.order_id
join products p on op.product_id = p.product_id
join depts d on p.dept_id = d.dept_id
where d.dept = 'dairy eggs')

-- COMMAND ----------

--- 40.	Find the top 5 aisles that have the highest number of products that were ordered only once.
with df as(
  select product_id, count(*) as total_orders from ord_prd group by product_id 
),
df2 as (
  select a.aisle, p.product_id
  from df d 
  join products p on d.product_id = p.product_id
  join aisles a on p.aisle_id = a.aisle_id
  where d.total_orders = 1 
)
select aisle, count(*) from df2 group by aisle order by count(*) desc

-- COMMAND ----------

---41.	Using a Common Table Expression (CTE) or subquery, calculate the percentage of orders from each department that contain only organic products.
with all_orders as(
  select distinct o.order_id, d.dept_id, d.dept from ord_prd op 
  join products p on op.product_id = p.product_id
  join orders o on op.order_id = o.order_id
  join depts d on p.dept_id = d.dept_id
),
non_organic as (
  select o.order_id,
  max(case when p.product_name not like '%Organic%' then 1 else 0 end) as non_org
  from ord_prd op join products p on op.product_id = p.product_id
  join orders o on op.order_id = o.order_id
  group by o.order_id
),
organic_only as (
  select ao.dept_id, ao.dept, count(distinct ao.order_id) as organic_orders
  from all_orders ao join non_organic no on ao.order_id = no.order_id
  where no.non_org = 0
  group by ao.dept_id, ao.dept
),
total_orders as (
  select dept_id, dept, count(distinct order_id) as total_orders
  from all_orders
  group by dept_id, dept
)
select t.dept_id, t.dept, t.total_orders, coalesce(o.organic_orders,0) as organic_only_orders,
round(coalesce(o.organic_orders,0) * 100.0 / t.total_orders, 3) as organic_pct
from total_orders t 
join organic_only o on t.dept_id = o.dept_id
order by organic_pct desc

-- COMMAND ----------

---42.	For each user, find the average days_since_prior_order for their orders placed on a Sunday, and compare it to their average days_since_prior_order for orders placed on a Wednesday.
  select user_id, 
  round(avg(case when order_dow = 0 then days_since_prior_order else null end),2) as avg_sunday,
  round(avg(case when order_dow = 3 then days_since_prior_order else null end),2) as avg_wednesday
  from orders 
  group by user_id

-- COMMAND ----------

---43.	Identify "super-shoppers" - users who have ordered from a different department in their last 3 consecutive orders
with ranked as (
  select user_id, order_id, order_number,
  rank()over(partition by user_id order by order_number desc) as rnk
  from orders
),
last_three_orders as (
  select * from ranked where rnk <= 3
),
departments as (
  select lto.user_id,lto.order_id, d.dept_id
  from ord_prd op 
  join last_three_orders lto on op.order_id = lto.order_id
  join products p on p.product_id = op.product_id
  join depts d on d.dept_id = p.dept_id
  group by lto.user_id,lto.order_id, d.dept_id
),
order_variety as (
  select user_id, count(distinct dept_id) as unq_dept_last_three
  from departments
  where user_id in (select user_id from last_three_orders group by user_id having count(distinct order_id) = 3)
  group by user_id
)
select count(*) from (select user_id from order_variety where unq_dept_last_three = 3) 

-- COMMAND ----------

---44.	Find the customers whose first order was their largest (in terms of unique products) and whose second order was their smallest.
WITH user_orders AS (
  SELECT 
    o.user_id,
    o.order_id,
    o.order_number,
    COUNT(DISTINCT op.product_id) AS unique_products
  FROM orders o
  JOIN ord_prd op ON o.order_id = op.order_id
  GROUP BY o.user_id, o.order_id, o.order_number
),

ranked_orders AS (
  SELECT *,
    RANK() OVER (PARTITION BY user_id ORDER BY unique_products DESC) AS rank_largest,
    RANK() OVER (PARTITION BY user_id ORDER BY unique_products ASC) AS rank_smallest
  FROM user_orders
),

first_second_orders AS (
  SELECT * 
  FROM user_orders
  WHERE order_number IN (1, 2)
),

summary AS (
  SELECT 
    uo.user_id,
    MAX(CASE WHEN order_number = 1 THEN unique_products END) AS first_order_count,
    MAX(CASE WHEN order_number = 2 THEN unique_products END) AS second_order_count
  FROM first_second_orders uo
  GROUP BY uo.user_id
),

rank_check AS (
  SELECT 
    ro.user_id,
    s.first_order_count,
    s.second_order_count,
    MAX(CASE WHEN ro.unique_products = s.first_order_count THEN ro.rank_largest END) AS is_largest,
    MAX(CASE WHEN ro.unique_products = s.second_order_count THEN ro.rank_smallest END) AS is_smallest
  FROM ranked_orders ro
  JOIN summary s ON ro.user_id = s.user_id
  GROUP BY ro.user_id, s.first_order_count, s.second_order_count
)
select count(*) from (
SELECT user_id
FROM rank_check
WHERE is_largest = 1 AND is_smallest = 1)


-- COMMAND ----------

---44.	Find the customers whose first order was their largest (in terms of unique products) and whose second order was their smallest.
explain(WITH user_orders AS (
  SELECT 
    o.user_id,
    o.order_id,
    o.order_number,
    COUNT(DISTINCT op.product_id) AS unique_products
  FROM orders o
  JOIN ord_prd op ON o.order_id = op.order_id
  GROUP BY o.user_id, o.order_id, o.order_number
),

ranked_orders AS (
  SELECT *,
    RANK() OVER (PARTITION BY user_id ORDER BY unique_products DESC) AS rank_largest,
    RANK() OVER (PARTITION BY user_id ORDER BY unique_products ASC) AS rank_smallest
  FROM user_orders
),

first_second_orders AS (
  SELECT * 
  FROM user_orders
  WHERE order_number IN (1, 2)
),

summary AS (
  SELECT 
    uo.user_id,
    MAX(CASE WHEN order_number = 1 THEN unique_products END) AS first_order_count,
    MAX(CASE WHEN order_number = 2 THEN unique_products END) AS second_order_count
  FROM first_second_orders uo
  GROUP BY uo.user_id
),

rank_check AS (
  SELECT 
    ro.user_id,
    s.first_order_count,
    s.second_order_count,
    MAX(CASE WHEN ro.unique_products = s.first_order_count THEN ro.rank_largest END) AS is_largest,
    MAX(CASE WHEN ro.unique_products = s.second_order_count THEN ro.rank_smallest END) AS is_smallest
  FROM ranked_orders ro
  JOIN summary s ON ro.user_id = s.user_id
  GROUP BY ro.user_id, s.first_order_count, s.second_order_count
)
select count(*) from (
SELECT user_id
FROM rank_check
WHERE is_largest = 1 AND is_smallest = 1))


-- COMMAND ----------

---45.	Calculate the average number of items per order, broken down by department and by order_hour_of_day.
select order_id, count(product_id) from ord_prd group by order_id 

-- COMMAND ----------

----46.	For each user, create a chronological list of their most frequently purchased department, with the count of orders for each department.
with dept as (
  select o.user_id, d.dept, count(*) as cnt
  from ord_prd op 
  join orders o on op.order_id = o.order_id
  join products p on p.product_id = op.product_id
  join depts d on d.dept_id = p.dept_id
  group by o.user_id, d.dept
)
select user_id, dept, cnt from dept order by user_id, dept 

-- COMMAND ----------

---47.	Identify the top 5 products that are almost always reordered (e.g., reordered = 1 in 99% of all their orders), and find the number of users who buy them.
with df as (
  select product_id,---
  sum(case when reordered = 1 then 1 else 0 end) as reorder,
  count(*) as total_order
  from ord_prd
  group by product_id
)
select product_id, 
round(cast(reorder as float)/ total_order,3) as reorder_rate
from df 
order by reorder_rate desc
limit 5

-- COMMAND ----------

----48.	For each day of the week and hour of the day combination, find the department_id that has the highest total number of products sold.
with df as (
  select o.order_dow as wk, o.order_hour_of_day as hr, p.dept_id,count(*) as cnt
  from orders o 
  join products p on o.order_id = p.product_id
  join ord_prd op on op.product_id = p.product_id
  group by o.order_dow, o.order_hour_of_day, p.dept_id
),
dg as (
select *, 
  rank() over(partition by wk, hr order by cnt desc) as rnk
from df
)
select wk, hr, cnt, dept_id from dg where rnk = 1 order by wk, hr, cnt desc 

-- COMMAND ----------

use default;
describe orders;

-- COMMAND ----------

