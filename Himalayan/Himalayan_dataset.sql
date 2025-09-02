-- Databricks notebook source
use catalog workspace

-- COMMAND ----------

use schema default 

-- COMMAND ----------

show tables in default 

-- COMMAND ----------

describe members

-- COMMAND ----------

describe peaks 

-- COMMAND ----------

describe refer 

-- COMMAND ----------

select count(*) from exped;
select count(*) from members;
select count(*) from peaks;
select count(*) from refer

-- COMMAND ----------

select (climbed_count/total_count) * 100 as climbed_percentage from 
(select count(*) as total_count, 
      count(case when pstatus = 'Climbed'
      then 1 end) as climbed_count from peaks)

-- COMMAND ----------

SELECT 
    fname,
    lname,
    COUNT(*) AS success_count
FROM members
WHERE msuccess = true
GROUP BY fname, lname 
HAVING COUNT(*) > 1
order by success_count desc

-- COMMAND ----------

describe exped

-- COMMAND ----------

select year, count(*) from exped group by year 

-- COMMAND ----------

delete from members where not(myear between 1900 and 2099)

-- COMMAND ----------

select count(*) from members

-- COMMAND ----------

select myear, count(*) from members group by myear order by count(*) desc

-- COMMAND ----------

select 
  count(case when sex = 'M' then 1 end) as males,
  count(case when sex = 'F' then 1 end) as females
from members

-- COMMAND ----------

select expid,Count(*) from members group by expid order by Count(*) desc

-- COMMAND ----------

select count(distinct expid) from members 

-- COMMAND ----------

select 89039/11444 as result 

-- COMMAND ----------

select expid, fname, lname, membid from members where sex = 'F' and status = 'Leader' group by expid,membid, fname, lname 

-- COMMAND ----------

select (732/11444)*100 as Womens_as_leader_of_expedition

-- COMMAND ----------

select expid, fname, lname, membid from members where sex = 'M' and status = 'Leader' group by expid,membid, fname, lname 

-- COMMAND ----------

SELECT COUNT(*) AS male_leader_count
FROM members
WHERE sex = 'M' AND status = 'Leader';

-- COMMAND ----------

select count(*) as female_success
from members
where sex = 'F' and status ='Leader' and msuccess = true;

-- COMMAND ----------

select (365/732)*100 as female_success_ratio

-- COMMAND ----------

select count(*) from members 
where sex = 'M' and status = 'Leader' and msuccess = true

-- COMMAND ----------

select (3817/10320)*100 as male_success_ratio

-- COMMAND ----------

select citizen, count(*) as no_of_people from members where hired = FALSE group by citizen order by count(*) desc limit 15

-- COMMAND ----------

select citizen, count(*) as no_of_death from members where death = TRUE group by citizen order by count(*) desc limit 15

-- COMMAND ----------

select citizen, count(*) as no_of_death from members where death = TRUE and hired = FALSE group by citizen order by count(*) desc limit 15

-- COMMAND ----------

select citizen, count(*) as no_of_death_hired from members where death = TRUE and hired = TRUE group by citizen order by count(*) desc limit 15

-- COMMAND ----------

select 
count(case when highpoint >8000 then 1 end) as no_of_exped_above_8000,
count(case when highpoint >7000 then 1 end) as no_of_exped_above_7000,
count(case when highpoint >6000 then 1 end) as no_of_exped_above_6000,
count(case when highpoint >5000 then 1 end) as no_of_exped_above_5000,
count(case when highpoint >4000 then 1 end) as no_of_exped_above_4000,
count(case when highpoint >3000 then 1 end) as no_of_exped_above_3000,
count(case when highpoint >2000 then 1 end) as no_of_exped_above_2000,
count(case when highpoint >1000 then 1 end) as no_of_exped_above_1000,
count(case when highpoint >0 then 1 end) as no_of_exped_above_0 
from exped 

-- COMMAND ----------

SELECT
  CASE
    WHEN highpoint > 8000 THEN '8001+ m'
    WHEN highpoint > 7000 THEN '7001_8000 m'
    WHEN highpoint > 6000 THEN '6001_7000 m'
    WHEN highpoint > 5000 THEN '5001_6000 m'
    WHEN highpoint > 4000 THEN '4001_5000 m'
    WHEN highpoint > 3000 THEN '3001_4000 m'
    WHEN highpoint > 2000 THEN '2001_3000 m'
    WHEN highpoint > 1000 THEN '1001_2000 m'
    WHEN highpoint > 0 THEN '1_1000 m'
    else '0'
    end as highpoint_range,
    count(*) as expedition_count
    from exped
  group by 1 order by expe


-- COMMAND ----------

delete from exped where season not in ("Autumn","Spring","Summer","Winter","Unknown")

-- COMMAND ----------

select season, count(*) as no_of_expedition from exped group by season order by count(*) desc

-- COMMAND ----------

--- Checking data consistency 

-- COMMAND ----------

select host, count(*) as no_of_expedition from exped group by host order by count(*) desc

-- COMMAND ----------

select COUNT(p.peakid) from peaks p left join exped e on p.peakid = e.peakid where e.peakid is null 

-- COMMAND ----------

select e.peakid from exped e left join peaks p on e.peakid = p.peakid where p.peakid is null

-- COMMAND ----------

delete from exped where peakid in ('BC(06/10,5250m),Smt(14/10)','8300m)')

-- COMMAND ----------

select e.peakid from exped e left join peaks p on e.peakid = p.peakid where p.peakid is null

-- COMMAND ----------

select m.peakid from members m left join peaks p on m.peakid=p.peakid where m.peakid is null

-- COMMAND ----------

select e.expid from exped e left join members m on e.expid=m.expid where e.expid is null

-- COMMAND ----------

select m.expid from members m left join exped e on m.expid=e.expid where m.expid is null

-- COMMAND ----------

select distinct sex from members

-- COMMAND ----------

select count(*) from members where sex not in ('M','F')

-- COMMAND ----------

select distinct e.year from exped e order by e.year desc

-- COMMAND ----------

select season, count(*) from exped group by season

-- COMMAND ----------

select host, count(*) from exped group by host 

-- COMMAND ----------

select count(*) from exped where bcdate is null

-- COMMAND ----------

select count(*) from exped where smtdate is null

-- COMMAND ----------

select count(*) from exped where bcdate is null and smtdate is null

-- COMMAND ----------

select count(*) from exped where bcdate>smtdate 

-- COMMAND ----------

select count(*) from exped where smtdays>totdays

-- COMMAND ----------

select count(*) from exped where bcdate > termdate

-- COMMAND ----------

select mseason, count(mseason) from members group by mseason

-- COMMAND ----------

update members set mseason = null where mseason not in ('Spring','Summer','Winter','Autumn')

-- COMMAND ----------

select mseason, count(mseason) from members group by mseason

-- COMMAND ----------

select sex, count(*) from members group by sex 

-- COMMAND ----------

update members set sex = null where sex not in('M','F')

-- COMMAND ----------

select yob, count(*) from members group by yob order by yob desc limit 5

-- COMMAND ----------

select expid, count(*) from members where status = 'Leader' group by expid having count(*) >1

-- COMMAND ----------

select expid, count(*) from members where status = 'Leader' and death = True group by expid order by count(*) desc 

-- COMMAND ----------

select count(distinct peakid) from peaks

-- COMMAND ----------

select peakid, count(*) from exped group by peakid order by count(*) desc

-- COMMAND ----------

use catalog workspace

-- COMMAND ----------

use schema default 

-- COMMAND ----------

select peakid, max(heightm) from peaks group by peakid order by max(heightm) desc limit 1;
select peakid, min(heightm) from peaks group by peakid order by min(heightm) asc limit 1

-- COMMAND ----------

(select peakid, max(heightm) from peaks group by peakid order by max(heightm) desc limit 1) union all
(select peakid, min(heightm) from peaks group by peakid order by min(heightm) asc limit 1)

-- COMMAND ----------

select peakid, max(year) from exped group by peakid order by max(year)

-- COMMAND ----------

select peakid, 
count(case when msuccess = true then 1 end) as sucess,
count(*) as total_attempts,
round((count(case when msuccess = true then 1 end)/count(*))*100,2)  as success_rate 
from members
group by peakid 
order by success_rate desc

-- COMMAND ----------

select mseason, 
count(case when msuccess=True then 1 end)as success,
count(*) as total,
round((count(case when msuccess=True then 1 end)/count(*))*100,2) as sucess_rate
from members 
group by mseason

-- COMMAND ----------

select count(case when mo2climb = true then 1 end) as oxy_used,
count(*),
round((count(case when mo2climb = true then 1 end)/(count(*)))*100,2) as use_rate
from members

-- COMMAND ----------

select count(case when mo2climb = true and msuccess = true then 1 end) as oxy_used,
count(case when msuccess = true then 1 end) as total,
round((count(case when mo2climb = true and msuccess = true then 1 end)/(count(case when msuccess = true then 1 end)))*100,2) as use_rate
from members

-- COMMAND ----------

select round((count(case when mo2climb = true and msuccess = true then 1 end)/(count(case when msuccess = true then 1 end)))*100,2) as use_rate
from members

-- COMMAND ----------

SELECT ROUND(
  (1.0 * COUNT(CASE WHEN mo2climb = true AND msuccess = true THEN 1 END) 
   / NULLIF(COUNT(CASE WHEN msuccess = true THEN 1 END), 0)) * 100, 
  2
) AS use_rate
FROM members;

-- COMMAND ----------

select peakid, 
ROUND((1.0 * COUNT(CASE WHEN death = true THEN 1 END) / COUNT(*)) * 100,2 ) AS death_rate 
from members 
group by peakid 
order by death_rate desc

-- COMMAND ----------

select distinct count(*) from exped

-- COMMAND ----------

alter table exped add column is_success boolean

-- COMMAND ----------

update exped 
set is_success = case when termreason = 'Success (main peak)' then true else false end

-- COMMAND ----------

select count(case when claimed = true or disputed =true then 1 end) as disputed,
count(case when is_success = true then 1 end) as success,
count(case when claimed = true or disputed =true and is_success = true then 1 end) as conflicted 
from exped 

-- COMMAND ----------

select occupation, count(*) as climbers  from members where status='Climber' group by occupation order by count(*) desc

-- COMMAND ----------

--- Basic queries 

-- COMMAND ----------

---1.	List the top 10 peaks with their height in meters and feet.
select peakid, heightm, heightf from peaks order by heightm desc limit 10

-- COMMAND ----------

---2.	Find all expeditions that happened in the summer season.
select expid from exped where season = 'Summer'

-- COMMAND ----------

---3.	Count the number of expeditions led by each country.
select nation, count(*) from exped group by nation 

-- COMMAND ----------

---4.	Get the distinct years when expeditions took place.
select distinct year from exped 

-- COMMAND ----------

---5.	Show all members who died on expeditions (name, peakid, expid, deathdate).
select m.fname, m.lname, e.peakid, m.expid, m.deathdate from exped e left join members m on e.expid=m.expid where death = true 

-- COMMAND ----------

---6.	List the peaks that have a firstasc value as Y (first ascents).

-- COMMAND ----------

---7.	Retrieve all expeditions where sponsor is not null.
select expid from exped where sponsor is not null 

-- COMMAND ----------

---8.	Show how many members joined expeditions for each peak.
select peakid, count(*) from members group by peakid order by count(*) desc 

-- COMMAND ----------

--- 9.	For each peak, count the number of successful expeditions (success = Y).
select peakid, 
count(case when is_success = true then 1 end) as successfull_expeditions,
count(*) as total_expeditions
from exped 
group by peakid 
order by count(*) desc

-- COMMAND ----------

--- 10.	Find the expedition(s) with the largest number of members.
select expid, count(*) from members group by expid order by count(*) desc limit 1

-- COMMAND ----------

---11.	Show the average height of peaks climbed each year.
select myear, round(avg(mperhighpt),2) as avg_height from members group by myear order by myear desc 

-- COMMAND ----------

--- 12.	List all expeditions where deaths occurred (members.dstatus = D).
select expid from exped where mdeaths >0

-- COMMAND ----------

---13.	Retrieve the number of expeditions that used oxygen vs those that didn’t.
select count(case when o2used = true then 1 end) as o2_used,
count(case when o2used = false  then 1 end) as o2_not_used
from exped

-- COMMAND ----------

--- 14.	Find all peaks that have never been successfully climbed.
select distinct pstatus from peaks 

-- COMMAND ----------

select peakid from peaks where pstatus = 'Unclimbed'

-- COMMAND ----------

---15.	Show the total number of members grouped by occupation.
select occupation, count(*) from members group by occupation order by count(*) desc 

-- COMMAND ----------

--- 16.	Find the earliest and latest summit dates recorded per peak.
select peakid, max(smtdate) as last_summit, min(smtdate) as first_summit from exped where is_success = true group by peakid 

-- COMMAND ----------

---17.	Rank the peaks by expedition count (most climbed → least climbed).
select peakid, 
count(*),
rank() over (order by count(*) desc) as peak_rank
from exped
group by peakid

-- COMMAND ----------

select peakid, count(*), dense_rank() over (order by count(*)desc) as r_peak from exped group by peakid 

-- COMMAND ----------

select peakid, nation, count(*) as expedition_count, rank() over (partition by nation order by count(*) desc) as r_peaks
from exped
group by peakid, nation

-- COMMAND ----------

--- top 5 nations on each peak with rank 
select *
from (
select peakid, nation, count(*) as no_exped,
rank() over (partition by peakid order by count(*)desc) as ranked_nation 
from exped 
group by peakid, nation)
as df
where ranked_nation <3
order by peakid,ranked_nation


-- COMMAND ----------

--- top 5 peaks of-- each nation
select * 
from (
  select nation, peakid, count(*) as no_exped, 
rank() over (partition by nation order by count(*) desc) as ranked_peak
from exped 
group by nation, peakid)
as df 
where ranked_peak <5
order by nation, ranked_peak 

-- COMMAND ----------

select * 
from (
  select nation,peakid, count(*) as no_exped, 
rank() over (partition by nation order by count(*) desc) as ranked_peak
from exped 
group by nation, peakid)
as df 
where nation = 'India' order by ranked_peak desc limit 5

-- COMMAND ----------

---18.	Find the top 5 peaks with the highest summit success rates (successful expeditions ÷ total expeditions).
select peakid,
count(case when is_success = true then 1 end) as success,
count(*) exped_done,
round(count(case when is_success = true then 1 end)/count(*)*100,2) as Success_rate
from exped 
group by peakid
having count(*)>100
order by round(count(case when is_success = true then 1 end)/count(*)*100,2) desc limit 5

-- COMMAND ----------

---19.	Identify expeditions where more than 20% of members died.
select expid, round(try_divide(mdeaths,totmembers)*100,2) as death_rate from exped where round(try_divide(mdeaths,totmembers)*100,2)>20
order by death_rate desc

-- COMMAND ----------

--- 20.	For each country, find the year with the most expeditions.
select * from ( 
select nation, year, count(*) as exped_count,
rank() over (partition by nation order by count(*) desc) as ranked_year
from exped
group by nation, year) as df 
where ranked_year = 1
order by exped_count desc

-- COMMAND ----------

---22.	Show all expeditions where no member reached the summit, but the expedition is still marked as successful (data quality check).
select e.expid
from exped e join members m on e.expid=m.expid
group by e.expid, e.is_success
having max(m.msuccess)=false and e.is_success = true

-- COMMAND ----------

--- 23.	Using a window function, calculate the cumulative number of expeditions per year.
select year, count(*),
sum(count(*)) over (order by year) as cum_Exped
from exped 
where year is not null
group by year

-- COMMAND ----------

---24.	Find the members who participated in more than 3 expeditions across different peaks.
--- we didnt have member id ....so we cant 

-- COMMAND ----------

---25.	For each peak, calculate the mortality rate = (total deaths ÷ total members) * 100.
select peakid, round(try_divide(sum(mdeaths*100),sum(totmembers)),2) as death_rate from exped group by peakid

-- COMMAND ----------

-- 27. Compare success rates of expeditions with vs without oxygen by peak.
select peakid, 
  round(try_divide(count_if(is_success = true and o2used = true), count_if(o2used = true)) * 100, 2) as S_rate_w_O,
  round(try_divide(count_if(is_success = true and o2used = false), count_if(o2used = false)) * 100, 2) as S_rate_wi_O
from exped
group by peakid

-- COMMAND ----------

---28.	For each expedition, calculate the average member age (if birth year data exists), then rank expeditions by youngest vs oldest teams.
select expid, 
round(avg(year(msmtdate1)-cast(yob as int)),2) as avg_age from members 
where yob is not null
group by expid 
having round(avg(year(msmtdate1)-cast(yob as int)),2) is not null 
order by avg_age  

-- COMMAND ----------

--- Max and min age of each expedition 
select expid, max(age), min(age)
from (
select expid, 
round(year(msmtdate1) - cast(yob as int),2) as age 
from members
where yob is not null
) as df 
group by expid

-- COMMAND ----------

select * from exped limit 1

-- COMMAND ----------

