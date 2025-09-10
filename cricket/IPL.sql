-- Databricks notebook source
use schema default;

-- COMMAND ----------

select * from all_ipl limit 10

-- COMMAND ----------

---2.	Find the total number of runs scored by MS Dhoni.
select sum(runs_total) from ipl where batter = 'MS Dhoni';

-- COMMAND ----------

--3.	List the names of all players who won the "Player of the Match" award.
select distinct player_of_match from ipl

-- COMMAND ----------

---4.	Find all the matches played at the "Wankhede Stadium".
select * from ipl where venue like '%Wankhede%'

-- COMMAND ----------

---5.	Count the total number of balls bowled in all matches.
select count(*) from ipl

-- COMMAND ----------

---6.	List the top 5 teams with the most wins.
select match_winner, count(distinct match_id) from ipl group by match_winner order by count(distinct match_id) desc

-- COMMAND ----------

update ipl set 
   batting_team = case when batting_team = 'Royal Challengers Bangalore' then 'Royal Challengers Bengaluru'else batting_team end,
   toss_winner = case when toss_winner = 'Royal Challengers Bangalore' then 'Royal Challengers Bengaluru'else toss_winner end,
   match_winner = case when match_winner = 'Royal Challengers Bangalore' then 'Royal Challengers Bengaluru'else match_winner end
where 'Royal Challengers Bangalore' in (batting_team,toss_winner,match_winner);
update ipl set 
   batting_team = case when batting_team = 'Kings XI Punjab' then 'Punjab Kings'else batting_team end,
   toss_winner = case when toss_winner = 'Kings XI Punjab' then 'Punjab Kings'else toss_winner end,
   match_winner = case when match_winner = 'Kings XI Punjab' then 'Punjab Kings'else match_winner end
where 'Kings XI Punjab' in (batting_team,toss_winner,match_winner);
update ipl set 
   batting_team = case when batting_team = 'Delhi Daredevils' then 'Delhi Capitals'else batting_team end,
   toss_winner = case when toss_winner = 'Delhi Daredevils' then 'Delhi Capitals'else toss_winner end,
   match_winner = case when match_winner = 'Delhi Daredevils' then 'Delhi Capitals'else match_winner end
where 'Delhi Daredevils' in (batting_team,toss_winner,match_winner);

-- COMMAND ----------

select match_winner, count(distinct match_id) from ipl group by match_winner order by count(distinct match_id) desc

-- COMMAND ----------

---8.	Count how many times the toss winner chose to field.
select toss_winner, count(distinct match_id)  from ipl where toss_decision = 'field' group by toss_winner;
select count(distinct match_id)  from ipl where toss_decision = 'field';


-- COMMAND ----------

---9.	Find the total number of 'run out' dismissals.
select count(*) from ipl where wicket_kind = 'run out'

-- COMMAND ----------

---10.	List the top 10 bowlers who have bowled the most balls.
select bowler, count(*) from ipl group by bowler order by count(*) desc limit 10

-- COMMAND ----------

---11.	Select all the unique venues where matches were played.
select distinct venue from ipl

-- COMMAND ----------

---12.	Find the average runs scored per ball across all matches.
select match_id, round(sum(runs_total) / count(*),3) as avg_run_per_ball from ipl group by match_id order by round(sum(runs_total) / count(*),3) desc

-- COMMAND ----------

---13.	List all matches where the toss winner was also the match winner.
select match_id from ipl where toss_winner = match_winner group by match_id

-- COMMAND ----------

---14.	Count the number of matches played in each city.
select city, count(distinct match_id) from ipl group by city order by count(distinct match_id) desc

-- COMMAND ----------

---15.	Find the total number of runs scored in the 2025 season.
select sum(runs_total) from ipl where year(date) = 2025

-- COMMAND ----------

---16.	Find the total runs scored by each batsman.
select batter, sum(runs_total) from ipl group by batter order by sum(runs_total) desc

-- COMMAND ----------

---17.	List the top 10 bowlers with the most wickets (of any kind).
select bowler, count(wicket_kind) as no_of_wickets from ipl 
where wicket_kind not in ('run out', 'retired hurt','retired out','obstructing the field','hit wicket')
group by bowler order by count(wicket_kind) desc limit 10

-- COMMAND ----------

---18.	For each venue, find the total number of runs scored.
select venue, sum(runs_total) as total_run from ipl 
group by venue order by total_run desc

-- COMMAND ----------

---20.	List the total number of 'caught' dismissals for each bowler.
select bowler, count(wicket_kind) as total_caught from ipl where wicket_kind = 'caught' group by bowler order by total_caught desc

-- COMMAND ----------

---21.	Find the batsman who has scored the most runs in a single match.
select match_id,batter, sum(runs_total) as total_runs from ipl group by match_id, batter order by total_runs desc limit 1 

-- COMMAND ----------

---22.	Calculate the average number of runs per match.
select round(sum(runs_total) / count(distinct match_id),2) as avg_runs_per_match
from ipl

-- COMMAND ----------

select match_id, inning_team, bowling_team, 
count( distinct bowling_team), count(distinct inning_team) 
from ipl group by match_id, inning_team, bowling_team
having count( distinct bowling_team) > 1  or count( distinct inning_team) > 1
order by match_id

-- COMMAND ----------

select * from ipl limit 1

-- COMMAND ----------

---23.	Find the total number of 'caught' dismissals for each team.
select bowling_team, count(*) as total_caught_dismisaals from ipl 
where wicket_kind = 'caught'
group by bowling_team
order by total_caught_dismisaals desc

-- COMMAND ----------

update ipl set 
   batting_team = case when batting_team = 'Rising Pune Supergiant' then 'Rising Pune Supergiants'else batting_team end,
   toss_winner = case when toss_winner = 'Rising Pune Supergiant' then 'Rising Pune Supergiants'else toss_winner end,
   match_winner = case when match_winner = 'Rising Pune Supergiant' then 'Rising Pune Supergiants'else match_winner end,
   bowling_team = case when bowling_team = 'Rising Pune Supergiant' then 'Rising Pune Supergiants'else bowling_team end
where 'Rising Pune Supergiant' in (batting_team,toss_winner,match_winner,bowling_team);

-- COMMAND ----------

update ipl set 
   bowling_team = case when bowling_team = 'Royal Challengers Bangalore' then 'Royal Challengers Bengaluru'else bowling_team end
where 'Royal Challengers Bangalore' in (bowling_team);
update ipl set
   bowling_team = case when bowling_team = 'Kings XI Punjab' then 'Punjab Kings'else bowling_team end
where 'Kings XI Punjab' in (bowling_team);
update ipl set
   bowling_team = case when bowling_team = 'Delhi Daredevils' then 'Delhi Capitals'else bowling_team end
where 'Delhi Daredevils' in (bowling_team);

-- COMMAND ----------

---24.	List all the matches where the total runs were over 200.
select match_id, batting_team, sum(runs_total) as total_runs from ipl
group by match_id, batting_team
having total_runs >=200
order by total_runs desc

-- COMMAND ----------

---25.	Find the bowler who has conceded the most 'runs_extras'.
select bowler, sum(runs_extras) as extra_run from ipl 
group by bowler 
order by extra_run desc
limit 10

-- COMMAND ----------

---26.	Count the number of times each team won the toss.
select toss_winner, count(distinct match_id) as toss_win from ipl 
group by toss_winner 
order by toss_win desc

-- COMMAND ----------

---28.	List the total number of wickets (any kind) taken by each team.
select bowling_team, count(wicket_kind) as no_of_wkts from ipl
group by bowling_team 
order by no_of_wkts desc

-- COMMAND ----------

---29.	Find the batsman with the highest strike rate (runs per ball) with a minimum of 500 balls faced.
select batter, sum(runs_total) as total_runs, count(runs_total) as total_balls,
round(sum(runs_total) *100/count(runs_total),2) as strike_rate
from ipl
group by batter
having count(runs_total) >=500
order by strike_rate desc

-- COMMAND ----------

---30.	List the bowler who has taken the most wickets against a specific team (e.g., against Mumbai Indians).
select bowler, count(wicket_kind) from ipl 
where batting_team = 'Royal Challengers Bengaluru'
group by bowler
order by count(wicket_kind) desc

-- COMMAND ----------

---31.	Count the number of matches where the winner of the toss lost the match.
select count(distinct match_id) from ipl 
where toss_winner != match_winner

-- COMMAND ----------

---32.	Find the batsman who has scored the most runs from boundaries.
select batter, sum(runs_total) as total_runs from ipl 
where runs_total in (4,6)
group by batter
order by total_runs desc

-- COMMAND ----------

---33.	Calculate the average runs conceded by each bowler per over (economy rate).
select bowler,sum(runs_total) as total_runs,
count(runs_total) as total_bolls,
round(sum(runs_total) /((count(runs_total))/6),2) as economy  
from ipl
group by bowler 

-- COMMAND ----------

---34.	Find the players who have won 'Player of the Match' more than 5 times.
select player_of_match, count(distinct match_id) as no_of_awards from ipl
group by player_of_match
having no_of_awards >5
order by no_of_awards desc

-- COMMAND ----------

---35.	List the matches where no 'wicket_kind' was recorded.
select match_id, count(wicket_kind) from ipl
group by match_id
having count(wicket_kind) = 0

-- COMMAND ----------

---36.	Find the top 3 bowlers with the most wickets in each season.
with df as (
select year(date) as yr, bowler, count(wicket_kind) as no_of_wkts,
row_number() over(partition by year(date) order by count(wicket_kind) desc) as rn 
from ipl
group by bowler, year(date)
)
select yr, bowler, no_of_wkts from df where rn <= 3

-- COMMAND ----------

---37.	For each match, find the batsman who scored the most runs.
  with df as (
    select match_id, batter, sum(runs_total) as total_runs 
from ipl
group by match_id, batter 
  ),
  dg as (
    select * , 
    row_number() over(partition by match_id order by total_runs desc) as rn
    from df
  )
  select match_id, batter, total_runs from dg where rn = 1

-- COMMAND ----------

---38.	Find the bowler who has dismissed a particular batsman (e.g., Virat Kohli) the most number of times.
select bowler, count(wicket_kind) as wkts from ipl
where batter = 'V Kohli'
group by bowler
order by wkts desc

-- COMMAND ----------

---39.	List the top 5 opening partnerships (runs scored by the first two batsmen of a team) across all matches.
with  first_wkt as (
select match_id, batting_team,
min(over*6 + ball_in_over) as fst_wkt_ball
from ipl
where wicket_kind is not null
group by match_id, batting_team
),
opn_partnership as (
  select i.match_id, i.batting_team,
  first(i.batter) as opener_1,
  first(i.non_striker) as opener_2,
  sum(i.runs_total) as total_runs
  from ipl i 
  join first_wkt f on i.match_id = f.match_id
  and i.batting_team = f.batting_team
  and (i.over*6 + i.ball_in_over) > f.fst_wkt_ball
  group by i.match_id, i.batting_team
)
select * from opn_partnership order by total_runs 

-- COMMAND ----------

---40.	Find the average total runs scored in a match for each city.
select city, 
round(sum(runs_total)/count(distinct match_id),2) as avg_score 
from ipl
group by city

-- COMMAND ----------

---41.	Find the total runs scored on each ball number (e.g., ball_in_over 1, 2, 3, etc.) across all matches.
select ball_in_over, sum(runs_total) from ipl
group by ball_in_over 
order by ball_in_over

-- COMMAND ----------

---42.	List the matches where the toss winner and the match winner were the same team, but they chose to field first.
select match_id, toss_winner, match_winner from ipl
where match_winner = toss_winner 
 and toss_decision = 'field'
 group by match_id, toss_winner, match_winner

-- COMMAND ----------

---44.	For each inning_team, find the total number of runs scored from extras.
select batting_team, sum(runs_extras) from ipl
group by batting_team 

-- COMMAND ----------

---45.	Find the batsman who has scored the most number of sixes.
select batter, count(runs_batter) as sixes from ipl
where runs_batter = 6 
group by batter 
order by sixes desc

-- COMMAND ----------

----46.	List the bowlers who have dismissed a batsman more than once with a 'bowled' dismissal.
select bowler, batter, count(wicket_kind) as wkt
from ipl 
where wicket_kind = 'bowled'
group by bowler, batter 
having wkt > 1
order by wkt desc

-- COMMAND ----------

---47.	Find the number of times each player was dismissed by 'run out'.
select batter, count(wicket_kind) as run_out from ipl
where wicket_kind = 'run out'
group by batter 
order by run_out desc

-- COMMAND ----------

---48.	List the top 5 venues with the highest average total runs per match.
select venue,
round(sum(runs_total)/count(distinct match_id),2) as avg_score
from ipl
group by venue 
order by avg_score desc
limit 5

-- COMMAND ----------

---49.	Find the team that won the most matches while chasing a target.
select match_winner, count(distinct match_id) from ipl
where toss_decision = 'field'
group by match_winner
order by count(distinct match_id) desc

-- COMMAND ----------

---50.	Calculate the total runs scored by each batter and the total runs they have conceded as a bowler.
with batter as (
 select batter, sum(runs_total) as total_runs_batted
from ipl group by batter 
),
bowler as (
  select bowler, sum(runs_total) as total_runs_conceded
from ipl group by bowler
)
select b.batter as player, b.total_runs_batted, w.total_runs_conceded
from batter b 
join bowler w on b.batter = w.bowler 
where b.total_runs_batted > 0 and w.total_runs_conceded > 0
order by total_runs_batted desc 

-- COMMAND ----------

---51.	Find the batsman who has scored a century (100+ runs) in a single match.
select match_id, batter, sum(runs_total) from ipl 
group by match_id, batter
having sum(runs_total) >= 100
order by sum(runs_total) desc

-- COMMAND ----------

with df as (
select match_winner, count(distinct match_id) as win from ipl
group by match_winner
),
dg as (
select batting_team, count(distinct match_id) as played
from ipl 
group by batting_team
)
select batting_team as team, win, played,
round(win * 100/ played,2) as win_pct 
from df d 
join dg g on d.match_winner = g.batting_team
order by win_pct desc

-- COMMAND ----------

---53.	Find the bowler who has the best bowling figures (most wickets for the least runs) in a single match.
select match_id, bowler, sum(runs_total) as runs_conceded, count(wicket_kind) as wickets
from ipl
group by match_id, bowler
having wickets > 0
order by try_divide(runs_conceded,wickets) asc

-- COMMAND ----------

---54.	Calculate the percentage of total runs scored from boundaries (fours and sixes) for each team.
with df as (
select batting_team, sum(runs_batter) as boundary
from ipl
where runs_batter in (4,6)
group by batting_team
),
dg as (
select batting_team, sum(runs_total) as total_runs
from ipl
group by batting_team
)
select df.batting_team, round(df.boundary*100/dg.total_runs,2) as boundary_pct
from df
join dg on df.batting_team = dg.batting_team
order by boundary_pct desc


-- COMMAND ----------

---55.	Find the matches where the margin of victory (difference in total runs) was the smallest.
with df as (
  select match_id, batting_team, sum(runs_total) as total_runs,
  row_number() over(partition by match_id order by match_id) as rn
  from ipl 
  group by match_id, batting_team
),
dg as (
  select match_id,
  max(case when rn = 1 then batting_team end) as batting_first,
  max(case when rn = 1 then total_runs end) as total_runs_1,
  max(case when rn = 2 then batting_team end) as batting_second,
  max(case when rn = 2 then total_runs end) as total_runs_2
  from df 
  group by match_id
),
result as (
  select *,
  (total_runs_1 - total_runs_2) as run_diff,
  case when total_runs_1 > total_runs_2 then batting_first else batting_second end as winner 
  from dg 
  where total_runs_1 > total_runs_2
)
select * 
from result
where run_diff is not null
order by run_diff asc

-- COMMAND ----------

with df as (
  select match_id, batting_team, sum(runs_total) as total_runs,
  row_number() over(partition by match_id order by match_id) as rn
  from ipl 
  group by match_id, batting_team
),
dg as (
  select match_id,
  max(case when rn = 1 then batting_team end) as batting_first,
  max(case when rn = 1 then total_runs end) as total_runs_1,
  max(case when rn = 2 then batting_team end) as batting_second,
  max(case when rn = 2 then total_runs end) as total_runs_2
  from df 
  group by match_id
),
result as (
  select d.match_id, d.batting_first, d.total_runs_1, d.batting_second,
  d.total_runs_2, i.match_winner as winner,
  (d.total_runs_1 - d.total_runs_2) as run_diff
  from dg d 
  join ipl i on d.match_id = i.match_id
  where d.total_runs_1 > d.total_runs_2
  group by d.match_id, d.batting_first, d.total_runs_1, d.batting_second,
  d.total_runs_2, i.match_winner
)
select * 
from result
where run_diff is not null
order by run_diff asc

-- COMMAND ----------

---56.	For each inning_team and batter, find the total runs scored and the number of balls faced.
select batting_team, batter, sum(runs_total) as total_runs, count(*) as balls_faced
from ipl
group by batting_team, batter
order by total_runs desc

-- COMMAND ----------

---57.	Find the total number of wicket_kind for each bowler in each inning_team.
select bowler,bowling_team, count(wicket_kind) as wickets
from ipl
group by bowler,bowling_team
order by wickets desc

-- COMMAND ----------

---58.	Identify the bowler who has bowled the most number of maiden overs (0 runs conceded).
with df as(
select match_id, bowler, over, sum(runs_total) as runs_in_over
from ipl
group by match_id,bowler,over
)
  select bowler, count(*) as maidens 
  from df 
  where runs_in_over = 0
  group by bowler
order by  count(*) desc

-- COMMAND ----------

----59.	Calculate the run rate (runs per over) for each team in each match.
select match_id, batting_team,
round(sum(runs_total)*6/count(runs_total),2) as run_rate
from ipl
group by match_id, batting_team
order by match_id

-- COMMAND ----------

---60.	Find the batsman who has been a non_striker for the most number of balls.
select non_striker, count(*) 
from ipl
group by non_striker
order by count(*) desc

-- COMMAND ----------

---61.	List all bowlers who have taken a hat-trick (3 wickets in 3 consecutive balls in an over).
with wickets as (
  select match_id, bowling_team, bowler, over, ball_in_over,
    row_number() over (partition by match_id, bowler order by over, ball_in_over) as rn
  from ipl
  where wicket_kind is not null
),
w1 as (
  select * from wickets),
w2 as (
  select * from wickets),
w3 as (
  select * from wickets)
select 
  w1.match_id, w1.bowler,
  w1.over as start_over,
  w1.ball_in_over as start_ball
from w1
join w2 on w1.match_id = w2.match_id and w1.bowler = w2.bowler and w2.rn = w1.rn + 1
join w3 on w1.match_id = w3.match_id and w1.bowler = w3.bowler and w3.rn = w1.rn + 2
order by w1.match_id, w1.bowler

-- COMMAND ----------

---62.	Find the batsman with the highest average score (total runs / number of dismissals), with a minimum of 10 dismissals.
select batter, sum(runs_total),count(wicket_kind),
round(try_divide(sum(runs_total),count(wicket_kind)),2) as avg_score
from ipl
group by batter 
having count(wicket_kind) >= 10
order by avg_score desc

-- COMMAND ----------

----63.	Find the top 5 bowler-batter combinations who have faced each other the most.
select batter, bowler, count(*) as total_banter
from ipl
group by batter, bowler
order by total_banter desc
limit 5

-- COMMAND ----------

---64.	Find the team with the highest average total score in matches where they lost.
with df as (
select match_id, batting_team as team, sum(runs_total) as total from ipl
where match_winner != batting_team 
group by match_id, batting_team
)
select team, round(avg(total),2) from df
group by team
order by avg(total)

-- COMMAND ----------

---65.	List the matches where runs_extras were more than runs_batter in any given over.
select distinct match_id from (
select  match_id, bowling_team, over,sum(runs_extras),sum(runs_batter)
from ipl
group by match_id, bowling_team, over
having sum(runs_extras) > sum(runs_batter)
order by match_id)

-- COMMAND ----------

---66.	For each match_id, rank the batsmen based on the runs they scored.
select match_id, batter,sum(runs_total),
rank() over(partition by match_id order by sum(runs_total) desc) as rank
from ipl
group by match_id, batter
order by match_id, rank

-- COMMAND ----------

----67.	For each match_id, find the running total of runs for the batting team.
select match_id, batting_team,over, ball_in_over,runs_total,
sum(runs_total) over(partition by match_id,batting_team order by over,ball_in_over 
rows between unbounded preceding and current row) as running_total
from ipl
order by match_id,batting_team, over, ball_in_over

-- COMMAND ----------

----68.	Find the highest score for a batsman in each city.
with df as (
select match_id, city, batter, sum(runs_total) as runs_scored,
rank() over(partition by city order by sum(runs_total) desc) as rank
from ipl
where city is not null
group by match_id, city, batter
order by city
)
select * from df where rank = 1

-- COMMAND ----------

---69.	For each bowler, find the number of wickets they have taken, and the rank of each wicket by inning_team and over.
select match_id, bowler, bowling_team, count(wicket_kind) as wickets,
rank() over(partition by match_id, bowling_team order by count(wicket_kind) desc) as rank
from ipl
group by match_id, bowler, bowling_team

-- COMMAND ----------

---70.	For each batter, calculate their average runs scored in their last 5 innings.
with df as (
select match_id, date, batter, sum(runs_total) as runs,
row_number() over(partition by batter order by date desc) as rn
from ipl
group by match_id, date,batter
)
select batter, avg(runs) as avg_runs
from df 
where rn<=5
group by batter
order by avg_runs desc

-- COMMAND ----------

---71.	Create a VIEW named MatchSummary that shows match_id, date, toss_winner, match_winner, and the total runs for each inning.
create view MatchSummary as
select match_id,date, toss_winner, match_winner, batting_team, sum(runs_total) as total_runs
from ipl
group by match_id, date, toss_winner, match_winner,batting_team


-- COMMAND ----------

select * from MatchSummary
order by match_id

-- COMMAND ----------

---72.	Create a VIEW named BatsmanStats that lists the batter, total runs, and balls faced.
create view batsmanstats as 
select batter, sum(runs_total) as total_runs, count(*) as balls_faced
from ipl
group by batter

-- COMMAND ----------

select * from batsmanstats
order by total_runs desc

-- COMMAND ----------

---73.	Create a STORED PROCEDURE that takes a team_name as input and returns the total number of wins for that team.
create or replace function total_wins(team_name string)
returns int 
return (
  select count(distinct match_id) from ipl where match_winner = team_name
 )

-- COMMAND ----------

select total_wins('Mumbai Indians')

-- COMMAND ----------

---74.	Create a FUNCTION that takes a player_name as input and returns the total runs they have scored as a batsman.
create or replace function total_runs(player_name string)
returns int 
return(
  select sum(runs_total) 
  from ipl
  where batter = player_name
)

-- COMMAND ----------

select get_total_runs('MS Dhoni') AS total_runs;

-- COMMAND ----------

---75.	Create a STORED PROCEDURE that takes a venue and a team_name as input and returns the total number of matches won by that team at that specific venue.
create or replace function wins_at_venue(venues string, team_name string)
returns int 
return(
  select count(distinct match_id)
  from ipl
  where venue = venues and match_winner = team_name
)

-- COMMAND ----------

select wins_at_venue('Rajiv Gandhi International Stadium, Uppal', 'Royal Challengers Bengaluru')

-- COMMAND ----------

select * from ipl limit 5

-- COMMAND ----------

---76.	For each match and each inning, find the top 3 bowlers who took the most wickets.
with df as (
select match_id, bowling_team, bowler, count(wicket_kind) as wkts,
row_number() over(partition by match_id,bowling_team order by count(wicket_kind) desc) as rank
from ipl
where wicket_kind is not null
group by match_id, bowling_team,bowler
)
select * from df 
where rank <=3
order by match_id, bowling_team, rank

-- COMMAND ----------

---77.	Calculate the average runs scored per over for each inning_team in a given match, using a rolling window of 3 overs.
with df as (
select match_id, batting_team, over, sum(runs_total) as runs_in_over
  from ipl
  group by match_id, batting_team, over
)
select *, 
avg(runs_in_over) over(partition by match_id, batting_team order by over
rows between 2 preceding and current row) as rolling_avg
from df
order by match_id, batting_team, over

-- COMMAND ----------

---78.	In each match, find the batter with the highest score and then calculate the rank of all other batsmen's scores relative to that highest score.
select match_id, batter, sum(runs_total) as total_runs,
rank() over(partition by match_id order by sum(runs_total) desc) as rank
from ipl
group by match_id, batter
order by match_id, rank 

-- COMMAND ----------

---79.	For a specific match and team, find the running total of runs_total for each ball, and also find the runs_total of the next two balls.
with df as (
select match_id, batting_team,over, ball_in_over,sum(runs_total) as runs
from ipl
group by match_id, batting_team, over, ball_in_over
),
dg as (
select *,
sum(runs) over(partition by match_id, batting_team order by over, ball_in_over
rows between unbounded preceding and current row) as running_total
from df
)
select *, (
lead(runs,1) over(partition by match_id, batting_team order by over, ball_in_over) 
+ lead(runs,2) over(partition by match_id, batting_team order by over, ball_in_over))
as next_two_balls
from dg

-- COMMAND ----------

---80.	Find the highest individual score for each batter and their rank based on that score across all matches.
with df as (
  select match_id, batting_team, batter, sum(runs_total) as runs
  from ipl
  group by match_id, batting_team, batter
),
dg as (
  select *, rank() over(partition by batter order by runs desc) as rank
from df
)
select batter, runs from dg
where rank = 1
order by runs desc

-- COMMAND ----------

---81.	Create a stored procedure GetBattingCareerSummary that takes a player's name as an input and returns their total runs, number of fours, number of sixes, and number of fifties/hundreds.
create or replace function player_summary(player text)
returns table (runs int, fours int, sixes int, fifties int, hundreds int)
as $$
begin 
return query 
with palyer_balls as (select * from ipl where batter = player_name),
player_innings as ( select match_id, batting_team, sum(runs_total) as runs_in_innings from player_balls group by match_id, batting_team),
boundaries as (select count(case when runs_total = 4 then 1 end) as fours, count(case when runs_total = 6 then 1 end) as sixes from player_balls),
fifties_hundreds as (select count(case when runs_in_innings between 50 and 99 then 1 end) as fifties, count(case when runs_in_innings >= 100 then 1 end) as hundreds from player_innings),
total_runs_cte AS (select sum(runs_total) AS total_runs FROM player_balls)
select
      total_runs_cte.total_runs,boundaries.fours, boundaries.sixes,fifties_hundreds.fifties,fifties_hundreds.hundreds
    from total_runs_cte, boundaries, fifties_hundreds;
end;
$$  
--- databricks does not support procedures directly 

-- COMMAND ----------

-- replace 'player_name' with the actual player's name when running the query
with player_balls as (
    select * 
    from ipl 
    where batter = 'V Kohli'
),
player_innings as (
    select match_id, batting_team, sum(runs_total) as runs_in_innings 
    from player_balls 
    group by match_id, batting_team
),
boundaries as (
    select 
        count(case when runs_total = 4 then 1 end) as fours, 
        count(case when runs_total = 6 then 1 end) as sixes 
    from player_balls
),
fifties_hundreds as (
    select 
        count(case when runs_in_innings between 50 and 99 then 1 end) as fifties, 
        count(case when runs_in_innings >= 100 then 1 end) as hundreds 
    from player_innings
),
total_runs_cte as (
    select sum(runs_total) as total_runs 
    from player_balls
)
select
    total_runs_cte.total_runs as runs,
    boundaries.fours, 
    boundaries.sixes,
    fifties_hundreds.fifties,
    fifties_hundreds.hundreds
from total_runs_cte, boundaries, fifties_hundreds;

-- COMMAND ----------

with player_balls as (
    select * 
    from ipl 
    where batter in ('V Kohli', 'S Dhawan', 'RG Sharma', 'MS Dhoni', 'D Warner')
),
player_innings as (
    select batter, match_id, batting_team, sum(runs_total) as runs_in_innings 
    from player_balls 
    group by batter, match_id, batting_team
),
boundaries as (
    select batter,
        count(case when runs_total = 4 then 1 end) as fours, 
        count(case when runs_total = 6 then 1 end) as sixes 
    from player_balls
    group by batter
),
fifties_hundreds as (
    select batter,
        count(case when runs_in_innings between 50 and 99 then 1 end) as fifties, 
        count(case when runs_in_innings >= 100 then 1 end) as hundreds 
    from player_innings
    group by batter
),
total_runs_cte as (
    select batter, sum(runs_total) as total_runs 
    from player_balls
    group by batter
)
select
    t.batter,
    t.total_runs as runs,
    b.fours, 
    b.sixes,
    fh.fifties,
    fh.hundreds
from total_runs_cte t
left join boundaries b on t.batter = b.batter
left join fifties_hundreds fh on t.batter = fh.batter
order by t.total_runs desc;


-- COMMAND ----------

---82.	Create a stored procedure VenueWinningRecord that takes a venue as input and returns the win-loss record of all teams at that venue.
with matches_played as (
    select 
        venue, 
        batting_team as team,
        count(distinct match_id) as matches_played 
    from ipl
    group by venue, batting_team
),
win_loss as (
    select 
        venue, 
        batting_team as team,
        count(distinct case when match_winner = batting_team then match_id end) as wins,
        count(distinct case when match_winner != batting_team and match_winner is not null then match_id end) as losses
    from ipl
    group by venue, batting_team
)
select 
    m.venue,
    m.team,
    m.matches_played,
    coalesce(w.wins, 0) as wins,
    coalesce(w.losses, 0) as losses
from matches_played m
left join win_loss w 
    on m.venue = w.venue and m.team = w.team
order by m.team

-- COMMAND ----------


    select 
        venue, 
        batting_team as team,
        count(distinct match_id) as matches_played,
        count(distinct case when match_winner = batting_team then match_id end) as wins,
        count(distinct case when match_winner != batting_team and match_winner is not null then match_id end) as losses
    from ipl
    group by venue, batting_team
    order by team

-- COMMAND ----------

select * from ipl

-- COMMAND ----------

