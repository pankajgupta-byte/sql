-- Databricks notebook source
use default;

-- COMMAND ----------

alter table odi_bbb_25 rename to odi

-- COMMAND ----------

select count(*) from odi

-- COMMAND ----------

select * from odi limit 5

-- COMMAND ----------

select distinct team_bat from odi

-- COMMAND ----------

select ball, count(*) 
from odi 
group by ball

-- COMMAND ----------

---1.	What is the total number of runs scored from wide balls in the entire dataset?
select sum(wide) from odi

-- COMMAND ----------

---2.	Find the total runs scored for each unique match (p_match).
select p_match, sum(score) from odi
group by p_match

-- COMMAND ----------

---3.	List all unique ground names where matches have been played.
select ground, count(distinct p_match) from odi 
group by ground
order by count(distinct p_match) desc  

-- COMMAND ----------

---4.	Count the total number of wickets that have fallen in the dataset.
select count(out) from odi 
where out = true 

-- COMMAND ----------

---5.	Find the top 5 batters (bat) with the highest total run.
select bat, sum(score) from odi
group by bat
order by sum(score) desc
limit 5

-- COMMAND ----------

---6.	What is the average number of runs scored per over in all day match games?
select 
round(sum(score) *6 /count(ball),2) as run_rate
from odi 
where daynight = 'day match'

-- COMMAND ----------

---7.	Find the team_bat that has scored the most runs in a single inning.
select p_match, team_bat, sum(score) from odi
group by p_match, team_bat
order by sum(score) desc 

-- COMMAND ----------

---8.	Count the number of balls (inns_balls) bowled by each bowler (bowl).
select bowl as bowler, count(*) from odi 
group by bowl
order by count(*) desc 

-- COMMAND ----------

---9.	Find all matches where a team won by winning the toss and choosing to bat first.
select distinct p_match from  odi 
where toss= winner and inns = 1

-- COMMAND ----------

---10.	Calculate the total number of balls bowled in each match.
select p_match, count(*) from odi 
group by p_match

-- COMMAND ----------

---11.	What is the average number of wickets per inning across all matches?
select round(count(out)/(count(distinct p_match) *2),2) as avg_wkts from odi
where out = true

-- COMMAND ----------

---12.	Find the top 10 batters based on their total number of balls faced (cur_bat_bf).
select bat as batter, count(*) from odi 
group by bat 
order by count(*) desc
limit 10

-- COMMAND ----------

---13.	Which country has hosted the most matches?
select country, count(distinct p_match) from odi
group by country 
order by count(distinct p_match) desc 

-- COMMAND ----------

---14.	Find the total runs scored by a specific batter and a specific bowler  against each other.
select bat, bowl, sum(score), count(*) as balls_faced from odi
group by bat, bowl 
order by sum(score) desc 

-- COMMAND ----------

---15.	Count how many times each type of dismissal has occurred.
select dismissal, count(*) from odi
where dismissal is not null 
group by dismissal
order by count(*) desc

-- COMMAND ----------

---16. Use a subquery to find the batter who has the highest individual score in a single inning.
select p_match, bat, runs from (
    select p_match, bat, sum(score) as runs from odi 
    group by p_match, bat
) t
where runs = (
    select max(runs) from (
        select sum(score) as runs from odi 
        group by p_match, bat
    ) subquery
)

-- COMMAND ----------

---17.	Use a CTE to list all batters who have scored more than 100 runs in at least one inning, along with their highest score.
with df as (
  select p_match, bat, sum(score) as inns_score
  from odi 
  group by p_match, bat
  having sum(score) >= 100
)
select bat, max(inns_score) as highest_score 
from df 
group by bat
order by highest_score  desc

-- COMMAND ----------

---18.	Find the bowler with the best bowling average (runs conceded per wicket) who has taken at least 50 wickets
select bowl, count(dismissal) as wkts,
sum(score) as runs_conceded, round(try_divide(sum(score),count(dismissal)),2) as avg_bowling
from odi
group by bowl
having wkts >=50
order by avg_bowling 

-- COMMAND ----------

---19.	Using a correlated subquery, for each team, find the highest individual score achieved by one of their batters in a single inning.
with df as (
  select p_match, team_bat, bat, sum(score) as runs
  from odi 
  group by p_match, team_bat, bat
),
dg as (
  select *, rank() over(partition by team_bat order by runs desc) as rnk
  from df
  )
select team_bat, bat, runs as highest_score
from dg
where rnk = 1

-- COMMAND ----------

---20.	Find all matches where the total runs from extras were greater than the runs scored by the highest-scoring batter in that match.
with extras as (
select p_match, sum(noball + byes + wide + legbyes) as extras
from odi
group by p_match
),
batter as (
  select p_match, bat, sum(score) as runs, 
  row_number() over(partition by p_match order by sum(score) desc) as rn
  from odi 
  group by p_match, bat
),
top_batter as (
  select p_match, bat, runs
  from batter 
  where rn = 1
)
 select t.p_match, t.bat, e.extras, t.runs 
 from top_batter t 
 join extras e on t.p_match = e.p_match
 where e.extras > t.runs

-- COMMAND ----------

---21.	Identify the top 5 highest-scoring innings (by a single batter) and the p_match ID for each.
select p_match, bat, sum(score) as runs
from odi
where bat = 'Rohit Sharma'
group by p_match, bat
order by runs desc
limit 5

-- COMMAND ----------

---22.	Using a subquery with IN, find the ground where the top 5 highest-scoring innings were played.
select p_match, bat, ground, sum(score) 
from odi
group by p_match, bat, ground
order by sum(score) desc
limit 5

-- COMMAND ----------

---23.	Find all matches where the total runs scored in the first inning were exactly 100 runs more than the total runs scored in the second inning.
with fst as (
select p_match, team_bat, sum(score) as f_total
from odi
where inns = 1
group by p_match, team_bat
),
second as (
select p_match, team_bat, sum(score) as s_total
from odi
where inns = 2
group by p_match, team_bat
)
select f.p_match, f.team_bat as first_team, f.f_total as first_inning_runs , s.team_bat as second_team,s.s_total as second_inning_runs
from fst f
join second s on f.p_match = s.p_match  
where f.f_total = s.s_total + 100

-- COMMAND ----------

---24.	Using a CTE, find the strike rate for all batters who have faced at least 500 balls, and rank them.
with df as (
  select bat, sum(score) as runs, count(*) as balls_faced
  from odi 
  group by bat
  having count(*) >= 500                
),
dg as (
  select *, round(try_divide(runs,balls_faced)*100,2) as strike_rate,
  rank() over(order by round(try_divide(runs,balls_faced)*100,2) desc) as rank
  from df
)
select *  from dg

-- COMMAND ----------

---25.	Find the bowler who has dismissed a specific batter (Ricky Ponting) the most number of times.
select bowl, bat, count(distinct dismissal) as wkt from odi
group by bowl, bat
order by wkt desc 

-- COMMAND ----------

---26.	Using a subquery, find the average number of runs scored per ball in matches where the winner was different from the toss winner.
select round(avg(score),2) from odi 
where winner != toss

-- COMMAND ----------

---27.	Identify all batters who have a higher average score than the overall average score for all batters in the dataset.
select bat, round(sum(score)/count(distinct p_match),2) as avg_score 
from odi 
group by bat
having avg_score > (select sum(score)/count(dismissal) from odi)

-- COMMAND ----------

---28.	Using a nested CTE, first calculate each batter's total runs in each inning, and then find the average of their top 3 highest scores.
with df as (
  select bat, p_match, sum(score) as innings_score
  from odi
  group by bat, p_match
),
dg as (
  select bat, innings_score,
  row_number() over(partition by bat order by innings_score desc) as rnk
  from df
),
top_three as (
  select bat, innings_score
  from dg 
  where rnk <= 3
)
select bat, round(avg(innings_score),2) as top_three_avg
from top_three
group by bat
order by top_three_avg desc

-- COMMAND ----------

---29.	Find the number of times a batter scored 50+ runs and the winner was not their team.
with df as (
  select bat, team_bat, p_match, sum(score) as runs
  from odi
  group by bat, p_match, team_bat
  having runs >= 50
),
dg as (
  select distinct p_match, winner
  from odi
)
select d.bat, count(d.p_match) 
from df d
join dg o on d.p_match = o.p_match
where d.team_bat != o.winner
group by d.bat

-- COMMAND ----------

---30.	What is the average number of wickets taken by bowlers in matches that ended in a win for the fielding side by more than 50 runs?
select count(dismissal)/ count(distinct p_match) as avg_wkts
from odi
where winner != team_bat and target - inns_runs > 50
and inns = 2 and dismissal is not null

-- COMMAND ----------

---31.	Use a CTE to count the number of times a day match was won by the team batting second.
select team_bat, count(distinct p_match) as wons 
from odi
where daynight = 'day match' and winner = team_bat and inns = 2
group by team_bat

-- COMMAND ----------

---32.	Find all batters who have a higher average score than the average score of batters in their own team.
with df as (
select bat, team_bat, round(try_divide(sum(score),count(dismissal)),2) as bat_avg
from odi
group by bat, team_bat
),
dg as (
select team_bat, round(try_divide(sum(score),count(dismissal)),2) as team_avg_score
from odi 
group by team_bat
)
select d.bat, d.bat_avg, g.team_bat as team, g.team_avg_score
from df d 
join dg g on d.team_bat = g.team_bat
where d.bat_avg > g.team_avg_score

-- COMMAND ----------

---33.	Identify matches where a team won despite their highest-scoring batter getting out for less than 10 runs.
with batter_totals as (
  select p_match, team_bat, bat, sum(score) as runs
  from odi
  group by p_match, team_bat, bat
),
top_scorers as (
  select p_match, team_bat, bat, runs,
    row_number() over (partition by p_match, team_bat order by runs desc) as rn
  from batter_totals
),
winning_top as (
  select ts.p_match, ts.team_bat, ts.bat, ts.runs
  from top_scorers ts
  join (select distinct p_match, winner from odi) w
    on ts.p_match = w.p_match and ts.team_bat = w.winner
  where ts.rn = 1 and ts.runs < 10
)
select * from winning_top

-- COMMAND ----------

---34.	Find the most common 'wicket kind' for the top 5 wicket-takers in the dataset.
with df as (
  select bowl, count(*) as wkts,
  rank() over(order by count(*) desc) as rnk
  from odi
  where dismissal is not null
  group by bowl 
),
top_five as (
select * from df 
where rnk <= 5
)
select dismissal, count(*) 
from odi
where dismissal is not null and 
      bowl in (select bowl from top_five)
group by dismissal
order by count(*) desc

-- COMMAND ----------

---35.	Using a CTE, find the team with the highest average run rate in all matches where they won.
with df as (
select team_bat as team, p_match, sum(score) as runs,count(*) as balls
from odi
where winner = team_bat
group by team_bat, p_match
)
select team , round((sum(runs)*6)/sum(balls),2) as run_rate 
from df 
group by team

-- COMMAND ----------

---36.	Using a window function, calculate the cumulative runs for a batter in an inning, ball by ball.
select p_match, team_bat, bat, over, ball, score,
sum(score) over(partition by p_match, team_bat, bat order by over , ball
rows between unbounded preceding and current row) as cum_score
from odi

-- COMMAND ----------

---37.	Find the top 3 bowlers in each match based on the number of wickets they took, using RANK() or DENSE_RANK().
with df as (
select p_match, bowl, count(*) as wkts,
row_number() over (partition by p_match order by count(*) desc, sum(score)) as rnk
from odi 
where dismissal is not null
group by p_match, bowl
)
select p_match, bowl, wkts
from df 
where rnk <=3

-- COMMAND ----------

---38.	Calculate the running total of inns_runs for the batting team, over by over.
select p_match, team_bat, over, ball,
sum(score) over(partition by p_match, team_bat order by over, ball
rows between unbounded preceding and current row) as running_total
from odi

-- COMMAND ----------

---39.	For each p_match, find the highest score and the number of balls faced for the batter who hit the winning run using a window function.
with df as (
  select p_match, bat, sum(score) as h_score
  from odi 
  group by p_match, bat
), 
dg as (
  select *, 
  row_number() over(partition by p_match order by h_score desc) as rnk
  from df
),
ds as (
  select * from dg 
  where rnk = 1
), 
lst as (
  select p_match, bat, over, ball, score,
  row_number() over(partition by p_match order by over desc, ball desc) as last_ball
  from odi 
  where winner = team_bat and inns = 2
),
lg as (
  select p_match, bat, score 
  from lst 
  where last_ball = 1
)
select d.p_match, d.bat, d.h_score, l.bat as last_batter, l.score as last_ball_score
from ds d
join lg l on d.p_match = l.p_match

-- COMMAND ----------

---40.	Use LEAD() and LAG() to find the runs scored on the ball immediately following a wicket.
select p_match, over, ball,
lead(score) over(partition by p_match order by over, ball) as next_ball_runs
from odi 
where dismissal is not null 

-- COMMAND ----------

---41.	What is the moving average of the required run rate (inns_rrr) over a 10-ball window for a specific match?
---it's given 

-- COMMAND ----------

---42.	For each batter, rank their individual innings scores from highest to lowest using a window function.
with df as (
  select bat, p_match, sum(score) as runs,
  rank() over(partition by bat order by sum(score) desc) as rnk
  from odi
  group by bat, p_match
)
select bat, runs, rnk
from df 
order by bat, runs desc

-- COMMAND ----------

---43.	Using NTILE(), divide all batters who have played at least 50 innings into 4 groups (quartiles) based on their total runs.
with df as (
select bat, sum(score) as runs
from odi
group by bat
having count(distinct p_match) >=50
),
dg as (
  select *, ntile(4) over(order by runs desc) as quartile
  from df 
)
select round(avg(runs),2) as avg_runs
from dg
group by quartile

-- COMMAND ----------

---44.	Find the number of consecutive no run balls a batter faced in an inning.
with df as (
  select p_match, team_bat, bat, over, ball,
  row_number() over(partition by p_match, team_bat, bat order by over, ball) as rn
  from odi
  where outcome = 'no run'
),
dg as (
  select *, 
  rn - row_number() over(partition by p_match, team_bat, bat order by over, ball) as g
  from df 
),
fd as (
  select p_match, team_bat, bat, count(*) as dots 
  from dg
  group by p_match, team_bat, bat , g
), 
gd as (
  select p_match, team_bat, bat, max(dots) as dot
  from fd 
  group by p_match, team_bat, bat
)
select * from gd order by dot desc

-- COMMAND ----------

---45.	Using a window function, find the bowler who bowled the most maiden overs (0 runs conceded) in each match.
with df as (
  select p_match, bowl, over, sum(score) as runs
  from odi
  group by p_match, bowl, over
),
dg as (
  select p_match, bowl,
  count(case when runs = 0 then 1 end) as maiden_overs
  from df 
  where runs = 0
  group by p_match, bowl
),
fd as (
  select *,
  rank() over(partition by p_match order by maiden_overs desc) as rnk
  from dg
)
select p_match, bowl, maiden_overs as most_maiden_overs 
from fd 
where rnk = 1 
order by p_match, most_maiden_overs desc 

-- COMMAND ----------

---46.	For each team, calculate the average number of runs scored in overs 1-10, 11-40, and 41-50, respectively.
select team_bat as team,
round(avg(case when over <= 10 then score end)*6,2) as avg_1_powerplay,
round(avg(case when over between 11 and 40 then score end)*6,2) as avg_2_powerplay,
round(avg(case when over between 41 and 50 then score end)*6,2) as avg_3_powerplay
from odi
group by team_bat

-- COMMAND ----------

---47.	What is the inns_rr at the end of each over for all innings in the dataset?
select p_match, team_bat,
round(sum(score)/count(distinct over),2) as run_rate
from odi
group by p_match, team_bat
order by p_match

-- COMMAND ----------

---48.	Using FIRST_VALUE(), find the batter who faced the first ball of each inning.
select distinct p_match, team_bat, 
first_value(bat) over(partition by p_match, team_bat order by over, ball) as first_batter,
last_value(bat) over(partition by p_match, team_bat order by over, ball
rows between unbounded preceding and unbounded following) as last_batter
from odi
order by p_match

-- COMMAND ----------

---50.	For each p_match, count the number of wickets taken in each 10-over block (1-10, 11-20, etc.).
select p_match, team_bat,
count(case when over <= 10 then out end) as wickets_1,
count(case when over between 11 and 20 then dismissal end) as wickets_2,
count(case when over between 21 and 30 then dismissal end) as wickets_3,
count(case when over between 31 and 40 then dismissal end) as wickets_4,
count(case when over between 41 and 50 then dismissal end) as wickets_5
from odi
where dismissal is not null
group by p_match, team_bat
order by p_match

-- COMMAND ----------

---51.	What is the longest streak of matches where a specific bowler took at least one wicket?
with all_matches as (
  select distinct bowl, date from odi
),
wickets_per_match as (
  select bowl, date, count(case when dismissal is not null then 1 end) as wkts
  from odi
  group by bowl, date
),
matches_wkts as (
  select a.bowl, a.date, coalesce(w.wkts, 0) as wkts
  from all_matches a
  left join wickets_per_match w on a.bowl = w.bowl and a.date = w.date
),
flag_streaks as (
  select *,
    case when wkts > 0 then 1 else 0 end as took_wicket,
    row_number() over (partition by bowl order by date) -
    row_number() over (partition by bowl, case when wkts > 0 then 1 else 0 end order by date) as grp
  from matches_wkts
),
streak_counts as (
  select bowl, grp, count(*) as streak_length
  from flag_streaks
  where took_wicket = 1
  group by bowl, grp
)
select bowl, max(streak_length) as longest_wicket_streak
from streak_counts
group by bowl
order by longest_wicket_streak desc

-- COMMAND ----------

----52.	Using a window function, find the highest number of wickets a bowler took in a single match.
with df as (
select bowl, p_match, count(dismissal)  as wkts
from odi
where dismissal is not null
group by bowl, p_match
order by count(dismissal) desc
),
dg as (
  select *, 
  row_number() over(partition by p_match order by wkts desc) as rnk
  from df
)
select p_match, bowl as bowler , wkts from dg where rnk = 1

-- COMMAND ----------

---53.	For each ground, rank the teams based on their winning percentage.
select ground, team_bat as team,
count(distinct p_match) as matches_played ,
count(distinct winner) as won,
round(try_divide(count(distinct winner) *100,count(distinct p_match)),2) as win_pct
from odi
group by ground, team_bat


-- COMMAND ----------

---54.	Find the batter who was on strike for the most number of boundary balls (4s and 6s).
select bat, count(*) as boundary_balls 
from odi 
where outcome in ('four','six')
group by bat 
order by boundary_balls desc

-- COMMAND ----------

---55.	Using a window function, find the p_match and inns where a team scored more than 15 runs in a single over.
with df as (
select p_match, inns, over, sum(score)
from odi 
group by p_match, inns, over
having sum(score) > 15
)
select distinct p_match, inns 
from df 

-- COMMAND ----------

---56.	Identify all instances of a hat-trick (3 wickets on 3 consecutive balls).
with df as (
  select p_match, inns, bowl, over, ball, dismissal,
  row_number() over(partition by p_match, inns, bowl order by over, ball) as rn
  from odi
),
dg as (
  select d1.p_match, d1.inns, d1.bowl, d1.over as over1, d1.ball as ball1, d2.over as over2, d2.ball as ball2,
  d3.over as over3, d3.ball as ball3
  from df d1
  join df d2 on d1.p_match = d2.p_match and d1.inns = d2.inns and d1.bowl = d2.bowl and d1.rn + 1 = d2.rn
  join df d3 on d1.p_match = d3.p_match and d1.inns = d3.inns and d1.bowl = d3.bowl and d1.rn + 2 = d3.rn
  where d1.dismissal is not null and d2.dismissal is not null and d3.dismissal is not null
)
select * from dg
order by p_match, inns, over1, ball1

-- COMMAND ----------

with df as (
  select p_match, inns, bowl, over, ball, dismissal,
  row_number() over(partition by p_match, inns, bowl order by over, ball) as rn 
  from odi 
),
dg as (
  select d1.p_match, d1.inns, d1.bowl as bowler, 
  d1.over as over_1, d1.ball as ball_1,
  d2.over as over_2, d2.ball as ball_2,
  d3.over as over_3, d3.ball as ball_3, 
  d4.over as over_4, d4.ball as ball_4
  from df d1
  join df d2 on d1.p_match = d2.p_match and d1.inns = d2.inns and d1.bowl = d2.bowl and d1.rn + 1 = d2.rn
  join df d3 on d1.p_match = d3.p_match and d1.inns = d3.inns and d1.bowl = d3.bowl and d1.rn + 2 = d3.rn
  join df d4 on d1.p_match = d4.p_match and d1.inns = d4.inns and d1.bowl = d4.bowl and d1.rn + 3 = d4.rn
  where d1.dismissal is not null
    and d2.dismissal is not null
    and d3.dismissal is not null
    and d4.dismissal is not null
)
select * from dg 
order by p_match, inns, over_1, ball_1

-- COMMAND ----------

---57.	Find all instances where a batter was dismissed by the same bowler on two consecutive balls they faced.
--- qn is not clear 

-- COMMAND ----------

---58.	Identify the longest partnership in terms of balls faced for each match, listing the two batters involved.
with all_deliveries as (
  select p_match, inns, over, ball, bat, dismissal,
  row_number() over(partition by p_match, inns order by over, ball) as rn
  from odi
),
partnership_groups as (
  select *, 
  count(dismissal) over(partition by p_match, inns order by over, ball
  rows between unbounded preceding and current row) as wicket_no
  from all_deliveries
),
partnership_pairs as (
  select p_match, inns, wicket_no, min(rn) as start_rn, max(rn) as end_rn, count(*) as balls_faced
  from partnership_groups
  group by p_match, inns, wicket_no
),
batters_involved as (
  select p.p_match, p.inns, p.wicket_no, p.balls_faced, min(bat) as batter1, max(bat) as batter2
  from partnership_groups g
  join partnership_pairs p 
  on g.p_match = p.p_match 
  and g.inns = p.inns 
  and g.wicket_no = p.wicket_no
  group by p.p_match, p.inns, p.wicket_no, p.balls_faced
),
longest_per_match as (
  select *,
  row_number() over(partition by p_match order by balls_faced desc) as rnk
  from batters_involved
)
select p_match, inns, batter1, batter2, balls_faced
from longest_per_match
where rnk = 1
order by p_match

-- COMMAND ----------

--- shorter version
with df as (
  select p_match, inns, bat, over, ball, dismissal,
  count(case when dismissal is not null then 1 end) over(partition by p_match, inns order by over, ball
  rows between unbounded preceding and 1 preceding) as partner
  from odi 
),
dg as (
  select p_match, inns, partner, count(*) as balls_faced,
  min(bat) as batter_1, max(bat) as batter_2
  from df 
  group by p_match, inns, partner
),
fd as(
  select *, row_number() over(partition by p_match order by balls_faced desc) as rn
  from dg
)
select p_match, inns, batter_1, batter_2, balls_faced
from fd where rn = 1

-- COMMAND ----------

---59.	Find the number of times a batter was dismissed by the same bowler in three or more different matches.

  select bat, bowl, count(distinct p_match) as dismissed
  from odi 
  where dismissal is not null
  group by bat, bowl
  having dismissed >= 3

-- COMMAND ----------

---60.	Find all instances of a run-out where the p_out (player out) was the non-striker.
select *
from odi
where dismissal = 'run out'
  and p_out <>p_bat

-- COMMAND ----------

---61.	Find all matches where a batter was caught out by the same fielder twice.
--- there is no fielder column/data

-- COMMAND ----------

---62.	For each match, find the batter who faced the most dot balls 
with df as (
  select p_match, bat, count(*) as dots
  from odi 
  where score = 0
  group by p_match, bat
),
dg as (
  select *, row_number() over(partition by p_match order by dots desc) as rn
  from df
)
select p_match, bat, dots from dg where rn = 1

-- COMMAND ----------

----and the bowler who bowled them.
with df as (
  select p_match, bat, bowl,count(*) as dots
  from odi 
  where score = 0
  group by p_match, bat, bowl
),
dg as (
  select *, row_number() over(partition by p_match order by dots desc) as rn
  from df
)
select p_match, bat, bowl, dots from dg where rn = 1

-- COMMAND ----------

---63.	Find all instances of a p_bat (player_id) scoring runs and the outcome being no run. What does this tell you about data quality?
select * from odi where score > 0 and outcome = 'no run'
--data is good, in these context

-- COMMAND ----------

--64.	Find all instances where a wicket fell on a noball.
select * from odi where outcome = 'no ball' and dismissal is not null

-- COMMAND ----------

--65.	Find the bowler who has bowled the most maidens (overs with 0 runs conceded).
with df as (
select bowl, over, sum(score) as runs
from odi 
group by bowl, over 
having runs = 0
)
select bowl, count(*) as maidens 
from df 
group by bowl
order by maidens desc 

-- COMMAND ----------

--66.	Identify the total runs conceded and balls bowled by each bowler on their first ball of each over.
select bowl, sum(score) as runs_conceded , count(*) as balls_bowled
from odi 
where ball = 1
group by bowl


-- COMMAND ----------

--67.	Find the number of times a batter was run out by the same fielder in different matches.
-- no data

-- COMMAND ----------

--68.	Identify matches where a team lost despite scoring more runs from boundaries than their opponents.
with df as (
select p_match, winner,
sum(case when inns = 1 then score else 0 end) as first_inns_runs,
min(case when inns = 1 then team_bat  end) as first_inns_team,
sum(case when inns = 2 then score else 0 end) as second_inns_runs,
min(case when inns = 2 then team_bat  end) as second_inns_team
from odi 
where outcome in ('four', 'six')
group by p_match,winner
)
select * from df 
where winner != first_inns_team and first_inns_runs > second_inns_runs
or  winner != second_inns_team and second_inns_runs > first_inns_runs

-- COMMAND ----------

---69.	Find the highest number of runs scored by a batter in a losing effort.
with df as (
  select p_match,winner, bat, sum(score) as runs
  from odi 
  where winner != team_bat
  group by p_match,winner, bat
),
dg as (
  select *, row_number() over(partition by p_match order by runs desc) as rn
  from df 
) select * from dg where rn = 1

-- COMMAND ----------

---70.	What is the average number of runs scored in the over immediately following a wicket?
with df as(
  select distinct p_match, over, ball 
  from odi 
  where dismissal is not null
),
dg as (
  select p_match, (over + 1) as next_over
  from df
),
fd as (
  select o.p_match, g.next_over , sum(o.score) as runs
  from odi o 
  join dg g on g.p_match = o.p_match and o.over = g.next_over
  group by o.p_match, g.next_over
)
select avg(runs) from fd

-- COMMAND ----------

--71.	Find the batter who has a higher scoring rate against left-handed bowlers compared to right-handed bowlers.


-- COMMAND ----------

---72.	Find the top 10 most common bowler-batter dismissal combinations.
select bat, bowl, count(*)  as wkts 
from odi 
where dismissal is not null
and dismissal in ('caught','bowled','leg before wicket','stumped','hit wicket')
group by bat, bowl

-- COMMAND ----------

---73.	Identify all instances where a team won by a single wicket.
select p_match, count(dismissal) as wkts
from odi
where inns = 2  
and winner = team_bat
and dismissal is not null
group by p_match
having wkts = 9

-- COMMAND ----------

--74.	For each ground, what percentage of matches were won by the team that won the toss?
select ground, count(distinct p_match) matches_played,
count(distinct case when winner = toss then 1 end) toss_match_won,
round(try_divide(count(distinct case when winner = toss then 1 end),count(distinct p_match))*100,2) as pct_won
from odi
group by ground 

-- COMMAND ----------

---75.	Find the average runs scored per ball in the first inning versus the second inning of all matches.
select avg(score) as avg_inns_1 
from odi
where inns = 1
union all
select avg(score) as avg_inns_2 
from odi
where inns = 2

-- COMMAND ----------

select inns, 
avg(case when inns = 1 then score end) as avg_inns_1,
avg(case when inns = 2 then score end) as avg_inns_2
from odi
group by inns

-- COMMAND ----------

--76.	In-depth Batting Analysis: For each batter with at least 1,000 runs, calculate their average and strike rate in overs 1-10, 11-40, and 41-50, and rank them in each period.
with df as (
  select bat, sum(score) as runs, count(*) as balls,
  round(try_divide(sum(score),count(case when dismissal is not null then 1 end)),2) as avg_run,
  round(try_divide(sum(score),count(*))*100,2) as strike_rate
  from odi
  group by bat 
  having sum(score) >= 1000
), 
pp as (
  select bat, 
  sum(case when over between 1 and 10 then score end) as runs_pp1,
  sum(case when over between 11 and 40 then score end) as runs_pp2,
  sum(case when over between 41 and 50 then score end) as runs_pp3
  from odi 
  group by bat
),
wkt as (
  select bat,
  count(case when over between 1 and 10 and dismissal is not null then 1 end) as wkt_pp1,
  count(case when over between 11 and 40 and dismissal is not null then 1 end) as wkt_pp2,
  count(case when over between 41 and 50 and dismissal is not null then 1 end) as wkt_pp3
  from odi 
  group by bat
),
balls as (
  select bat,
  count(case when over between 1 and 10 then 1 end) as balls_pp1,
  count(case when over between 11 and 40 then 1 end) as balls_pp2,
  count(case when over between 41 and 50 then 1 end) as balls_pp3
  from odi 
  group by bat
),
dg as (
  select p.bat, 
  round(try_divide(p.runs_pp1,w.wkt_pp1),2) as avg_pp1,
  round(try_divide(p.runs_pp2,w.wkt_pp2),2) as avg_pp2,
  round(try_divide(p.runs_pp3,w.wkt_pp3),2) as avg_pp3,
  round(try_divide(p.runs_pp1,b.balls_pp1)*100,2) as sr_pp1,
  round(try_divide(p.runs_pp2,b.balls_pp2)*100,2) as sr_pp2,
  round(try_divide(p.runs_pp3,b.balls_pp3)*100,2) as sr_pp3
  from pp p
  join wkt w on p.bat = w.bat 
  join balls b on p.bat = b.bat
)
select d.bat, d.runs, d.balls, d.avg_run, d.strike_rate,
       g.avg_pp1, g.avg_pp2, g.avg_pp3,
       g.sr_pp1, g.sr_pp2, g.sr_pp3
from df d 
left join dg g on d.bat = g.bat

-- COMMAND ----------

--77.	Bowling Efficiency: Create a bowler_effectiveness_score using a formula  (total_wickets * 10) / (total_runs_conceded + 1) and rank the bowlers.
  select bowl, 
  round(try_divide(sum(case when dismissal is not null then 1 end) * 10,sum(score)),2) as bowler_effectiveness_score
  from odi 
  group by bowl

-- COMMAND ----------

with df as (
  select bowl, 
  sum(case when dismissal is not null then 1 end) as wkts, 
  sum(score) as runs 
  from odi 
  group by bowl
), 
eff as (
  select bowl, wkts, runs, 
  round(try_divide(wkts * 10, runs + 1), 2) as bowler_effectiveness_score 
  from df
) 
select *, rank() over(order by bowler_effectiveness_score desc) as effectiveness_rank 
from eff

-- COMMAND ----------

--78.	Match-Winning Contribution: For each match, identify the player who contributed the most to the win, defined as the batter with the highest score or the bowler with the most wickets.
with df as(
  select p_match, team_bat,bat, sum(score) as runs,
  row_number() over(partition by p_match, team_bat order by sum(score) desc) as rn
  from odi
  where team_bat = winner
  group by p_match, team_bat,bat
),
ts as (
  select * from df where rn = 1
),
bowler as (
  select p_match, team_bowl, bowl, count(dismissal) as wkts,
  row_number() over(partition by p_match, team_bowl order by count(dismissal) desc) as rna 
  from odi 
  where team_bowl = winner
  and dismissal is not null
  group by p_match, team_bowl,bowl
),
tb as (
  select * from bowler where rna = 1
) 
select s.p_match, s.team_bat as winner, s.bat as highest_scorer, s.runs,
       b.bowl as top_wkt_taker, b.wkts
from ts s
join tb b on s.p_match = b.p_match

-- COMMAND ----------

--79.	Pitch Report: Using the line and length columns, analyze which ground is most favorable for a specific bowling bowl_kind (e.g., pace, spin).
with df as (
  select ground, 
  round(try_divide(count(case when outcome in('no run','out') then 1 end),count(*))*100,2) as spin_dots_pct,
  round(try_divide(sum(score),count(case when dismissal is not null then 1 end)),2) as spin_avg_run,
  round(avg(score) * 6,2) as spin_economy_rate,
  round(try_divide(count(*),count(case when dismissal is not null then 1 end)),2) as spin_strike_rate
  from odi 
  where bowl_kind = 'spin bowler'
  group by ground
),
dg as (
  select ground, 
  round(try_divide(count(case when outcome in('no run','out') then 1 end),count(*))*100,2) as pace_dots_pct,
  round(try_divide(sum(score),count(case when dismissal is not null then 1 end)),2) as pace_avg_run,
  round(avg(score) * 6,2) as pace_economy_rate,
  round(try_divide(count(*),count(case when dismissal is not null then 1 end)),2) as pace_trike_rate
  from odi 
  where bowl_kind = 'pace bowler'
  group by ground
)
select d.ground, d.spin_dots_pct, d.spin_avg_run, d.spin_economy_rate, d.spin_strike_rate,
       g.pace_dots_pct, g.pace_avg_run, g.pace_economy_rate, g.pace_trike_rate
from df d
join dg g on d.ground = g.ground

-- COMMAND ----------

--80.	Team Strategy Analysis: Find the average number of runs scored in the first 5 overs of each team's inning. Rank the teams from fastest to slowest starters.
with df as (
select team_bat,
round(try_divide(sum(score), count(distinct(p_match))), 2) as avg_runs 
from odi 
where over between 1 and 5
group by team_bat
order by avg_runs desc
)
select *,
rank() over(order by avg_runs desc) as rank
from df

-- COMMAND ----------

--81.	Performance Under Pressure: Find the average required run rate (inns_rrr) for the winning team in the final 10 overs of a match they won. 
-- scoring rate
with df as (
  select team_bat as team, 
  sum(score) as runs_scored
  from odi 
  where over between 41 and 50
  and team_bat = winner 
  group by team_bat
),
dg as (
  select team_bat as team, p_match, over
  from odi 
  where over between 41 and 50
  and team_bat = winner 
  group by team_bat, p_match, over
),
fd as (
  select team, count(*) as total_overs
  from dg 
  group by team
)
select d.team, 
       round(try_divide(runs_scored, total_overs), 2) as actual_rr_in_41_50
from df d
join fd f on d.team = f.team
order by actual_rr_in_41_50 desc

-- COMMAND ----------

-- actual required run rate--
with first_inns as (
  select p_match, sum(score) as target
  from odi
  where inns = 1
  group by p_match
),
runs_after_40 as (
  select p_match, sum(score) as runs_scored
  from odi
  where inns = 2 and over <= 40
  group by p_match
),
winners as (
  select distinct p_match, team_bat
  from odi
  where inns = 2 and team_bat = winner
),
required_rrr as (
  select 
    w.team_bat as team,
    w.p_match,
    round(try_divide(f.target - r.runs_scored, 10), 2) as inns_rrr
  from winners w
  join first_inns f on w.p_match = f.p_match
  join runs_after_40 r on w.p_match = r.p_match
)
select team, round(avg(inns_rrr), 2) as avg_required_rrr
from required_rrr
group by team
order by avg_required_rrr desc


-- COMMAND ----------

---82.	The Unbreakable Record: Find the longest partnership in terms of balls faced (cur_bat_bf difference), along with the batters involved. This requires very careful state tracking.

with df as (
  select p_match, inns, bat, over, ball, dismissal,
  count(case when dismissal is not null then 1 end) over(partition by p_match, inns order by over, ball
  rows between unbounded preceding and 1 preceding) as rn
  from odi
),
dg as (
  select p_match, inns, rn, array_agg(distinct bat) as batters, 
  count(*) as partnership_balls_faced,
  row_number() over(partition by p_match order by count(*) desc) as rna
  from df 
  group by p_match, inns, rn
)
select p_match, batters, partnership_balls_faced from dg where rna = 1

-- COMMAND ----------

---83.	Wagon Wheel Analysis: Using wagonX and wagonY data, find the most common shot and length combination for a batter who scored a century.
-- lack of data

-- COMMAND ----------

--84.	Team-on-Team Dominance: For the top 5 most common head-to-head match-ups, 
with df as (
  select p_match, team_bat, team_bowl, winner 
  from odi 
),
dg as (
  select team_bat as team, team_bowl as opponent,
  count(distinct p_match) as matches_played,
  count(distinct case when winner = team_bat then p_match end) as wins
  from df
  group by team_bat, team_bowl
  union all
  select team_bowl as team, team_bat as opponent,
  count(distinct p_match) as matches_played,
  count(distinct case when winner = team_bowl then p_match end) as wins
  from df
  group by team_bat, team_bowl
)
select team, opponent, wins, matches_played,
round(try_divide(wins, matches_played), 2) as win_percentage
from dg

-- COMMAND ----------

with df as (
  select p_match, team_bat as team, team_bowl as opponent, bat as batter, score as runs_scored,
    case when dismissal is not null then 1 else 0 end as out_flag
  from odi
),
dg as (
  select team, opponent, batter, 
    sum(runs_scored) as total_runs, 
    sum(out_flag) as dismissals
  from df
  group by team, opponent, batter
),
dg1 as (
  select team, opponent, batter, total_runs, dismissals,
    case when dismissals = 0 then total_runs else total_runs * 1.0 / dismissals end as batting_avg,
    row_number() over (partition by team, opponent order by 
      case when dismissals = 0 then total_runs else total_runs * 1.0 / dismissals end desc
    ) as rn
  from dg
)
select team, opponent, batter, total_runs, dismissals, round(batting_avg, 2) as batting_avg
from dg1
where rn = 1

-- COMMAND ----------

--85.	Captain's Influence: Assuming the first batter listed for a team is their captain, find the average batting strike rate of all captains compared to the rest of the players on the team.
with df as (
  select team_bat as team, bat as batter,
  sum(score) as runs, 
  count(case when dismissal is not null then 1 end) as dismissals
  from odi group by team_bat, bat
),
teams as(
  select team, batter, runs, dismissals, 
  round(case when dismissals = 0 then runs else runs * 1.0 / dismissals end,2) as strike_rate
  from df
),
capt as (
  select team_bat,
  first_value(bat) over(partition by team_bat order by over, ball) as captain 
  from odi
  group by team_bat, bat, over, ball
), 
captain as (
  select team_bat, captain
  from capt 
  group by team_bat, captain
),
fd as (
  select * 
  from teams t 
  join captain c on t.team = c.team_bat
  where t.batter = c.captain 
)
select t.team, t.batter, t.runs, t.dismissals, t.strike_rate,
       f.batter, f.runs, f.runs, f.dismissals, f.strike_rate
from teams t  
join fd f on t.team = f.team_bat

-- COMMAND ----------

--86.	Streak Analysis: Find the longest streak of matches where a specific bowler took at least 3 wickets.
with df as (
  select bowl as bowler, date, 
  count(case when dismissal is not null then 1 end) as wkts 
  from odi 
  group by bowl, date 
  ), 
  dg as (
    select bowler, date, wkts,
    case when wkts >= 3 then 1 else 0 end as streak
    from df 
  ),
  dg1 as (
  select bowler, date, streak,
    sum(case when streak = 0 then 1 else 0 end) over (
      partition by bowler order by date 
      rows between unbounded preceding and current row
    ) as grp
  from dg
  ),
  dg2 as (
    select bowler, count(*) as s_len
    from dg1 
    where streak = 1 
    group by bowler, grp
  ),
  dg3 as (
    select bowler, max(s_len) as max_streak
    from dg2
    group by bowler
  )
  select * from dg3 order by max_streak desc 

-- COMMAND ----------

