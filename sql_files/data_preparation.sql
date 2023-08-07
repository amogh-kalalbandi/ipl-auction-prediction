truncate table ipl_stats.batsmen_statistics;
truncate table ipl_stats.bowler_statistics;

CREATE TABLE ipl_stats.batsmen_statistics (
    Id INTEGER,
    Player VARCHAR(255),
    Matches INTEGER,
    Innings INTEGER,
    Number_of_Not_Outs INTEGER,
    Runs INTEGER,
    Highest_score VARCHAR(255),
    Average NUMERIC(18,9),
    Balls_Faced INTEGER,
    strike_rate NUMERIC(18,9),
    number_of_hundreds INTEGER,
    number_of_fifties INTEGER,
    number_of_fours INTEGER,
    number_sixes INTEGER

);

drop table if exists ipl_stats.bowler_statistics;
CREATE TABLE ipl_stats.bowler_statistics (
    Id INTEGER,
    Player VARCHAR(255),
    Matches INTEGER,
    Innings INTEGER,
    Number_Of_Overs NUMERIC(18,1),
    Runs INTEGER,
    Wickets INTEGER,
    Best_bowling_figures VARCHAR(255),
    Average NUMERIC(18,9),
    Economy NUMERIC(18,9),
    Strike_rate NUMERIC(18,9),
    number_of_four_fers INTEGER,
    number_of_five_fers INTEGER
);


COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2016.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2017.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2018.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2019.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2020.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2021.csv'
DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS ipl_stats.cumulated_batsmen_statistics;
CREATE TABLE ipl_stats.cumulated_batsmen_statistics AS
SELECT
    player,
    matches,
    innings,
    number_of_not_outs,
    tot_runs,
    highest_score,
    tot_balls_faced,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_of_sixes,
    (cast(tot_runs as numeric) / cast(matches as numeric)) as average,
    ((cast(tot_runs as numeric) / cast(tot_balls_faced as numeric))*100) as strike_rate
FROM (
    select player,
        sum(matches) as matches,
        sum(innings) as innings,
        sum(number_of_not_outs) as number_of_not_outs,
        sum(runs) as tot_runs,
        max(REGEXP_REPLACE(highest_score, '\*+', '')::int) as highest_score,
        sum(balls_faced) as tot_balls_faced,
        sum(number_of_hundreds) as number_of_hundreds,
        sum(number_of_fifties) as number_of_fifties,
        sum(number_of_fours) as number_of_fours,
        sum(number_sixes) as number_of_sixes
    from ipl_stats.batsmen_statistics
    group by player
    order by player
) a1;

--------------------------------------------------------------

truncate table ipl_stats.bowler_statistics;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2016.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2017.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2018.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2019.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2020.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2021.csv'
DELIMITER ',' CSV HEADER;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2022.csv'
DELIMITER ',' CSV HEADER;


UPDATE ipl_stats.bowler_statistics
SET number_of_balls_bowled = number_of_overs*6;

alter table ipl_stats.bowler_statistics ADD column max_number_of_wickets_per_match INTEGER;
alter table ipl_stats.bowler_statistics ADD column number_of_balls_bowled INTEGER;

UPDATE ipl_stats.bowler_statistics
SET max_number_of_wickets_per_match = split_part(best_bowling_figures, '/', 1)::integer;


DROP TABLE IF EXISTS ipl_stats.cumulated_bowlers_statistics;
CREATE TABLE ipl_stats.cumulated_bowlers_statistics AS
select
    player,
    matches,
    innings,
    number_of_overs,
    runs,
    wickets,
    number_of_four_fers,
    number_of_five_fers,
    max_number_of_wickets_per_match,
    number_of_balls_bowled,
    (cast(runs as FLOAT)/wickets) as average,
    (cast(number_of_balls_bowled as float)/wickets) as strike_rate
FROM (
    SELECT
        player,
        sum(matches) as matches,
        sum(innings) as innings,
        sum(number_of_overs) as number_of_overs,
        sum(runs) as runs,
        sum(wickets) as wickets,
        max(number_of_four_fers) as number_of_four_fers,
        max(number_of_five_fers) as number_of_five_fers,
        max(max_number_of_wickets_per_match) as max_number_of_wickets_per_match,
        sum(number_of_balls_bowled) as number_of_balls_bowled
    FROM ipl_stats.bowler_statistics
    group by player
    order by player
) a1;


-- the following 2 queries are run only for 2022 data, because the data is reveresed in it.
UPDATE ipl_stats.bowler_statistics
SET max_number_of_wickets_per_match = split_part(best_bowling_figures, '/', 2)::integer
where max_number_of_wickets_per_match > 10;

UPDATE ipl_stats.bowler_statistics
SET max_number_of_wickets_per_match = split_part(best_bowling_figures, '/', 2)::integer
where max_number_of_wickets_per_match > 5
and player not in ('Alzarri Joseph', 'Adam Zampa');


-- Clean 2022 bowler data.


--------------

drop table if exists ipl_stats.auction_data;
create table ipl_stats.auction_data(
    Player VARCHAR(255),
    Role VARCHAR(255),
    Amount BIGINT,
    Team VARCHAR(255),
    Year INTEGER,
    Player_Origin VARCHAR(255)
);


COPY ipl_stats.auction_data(
    Player,
    Role,
    Amount,
    Team,
    Year,
    Player_Origin
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPLPlayerAuctionData.csv'
DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS ipl_stats.transformed_auction_data;
CREATE TABLE ipl_stats.transformed_auction_data as
SELECT player, role, amount, team, year, player_origin
FROM (
    SELECT *, row_number() OVER (PARTITION BY player ORDER BY year desc) as row_num
    FROM ipl_stats.auction_data
    WHERE year != 2022
) a1
WHERE a1.row_num = 1;



----------------------------------------------------------



-- Prediction data.
COPY ipl_stats.batsmen_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_of_Not_Outs,
    Runs,
    Highest_score,
    Average,
    Balls_Faced,
    strike_rate,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_sixes)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Batting Stats/BATTING_STATS_IPL_2022.csv'
DELIMITER ',' CSV HEADER;


CREATE TABLE ipl_stats.prediction_batsmen_stats_data AS
SELECT
    player,
    matches,
    innings,
    number_of_not_outs,
    tot_runs,
    highest_score,
    tot_balls_faced,
    number_of_hundreds,
    number_of_fifties,
    number_of_fours,
    number_of_sixes,
    (cast(tot_runs as numeric) / cast(matches as numeric)) as average,
    ((cast(tot_runs as numeric) / cast(tot_balls_faced as numeric))*100) as strike_rate
FROM (
    select player,
        sum(matches) as matches,
        sum(innings) as innings,
        sum(number_of_not_outs) as number_of_not_outs,
        sum(runs) as tot_runs,
        max(REGEXP_REPLACE(highest_score, '\*+', '')::int) as highest_score,
        sum(balls_faced) as tot_balls_faced,
        sum(number_of_hundreds) as number_of_hundreds,
        sum(number_of_fifties) as number_of_fifties,
        sum(number_of_fours) as number_of_fours,
        sum(number_sixes) as number_of_sixes
    from ipl_stats.batsmen_statistics
    group by player
    order by player
) a1;

COPY ipl_stats.bowler_statistics(
    Id,
    Player,
    Matches,
    Innings,
    Number_Of_Overs,
    Runs,
    Wickets,
    Best_bowling_figures,
    Average,
    Economy,
    Strike_rate,
    number_of_four_fers,
    number_of_five_fers
)
FROM '/Users/amoghkulkarni/Documents/RS/machine-learning-zoomcamp/data_sources/IPL Player Stats/Bowling Stats/BOWLING_STATS_IPL_2022.csv'
DELIMITER ',' CSV HEADER;


UPDATE ipl_stats.bowler_statistics
SET number_of_balls_bowled = number_of_overs*6
WHERE number_of_balls_bowled IS NULL;


UPDATE ipl_stats.bowler_statistics
SET max_number_of_wickets_per_match = split_part(best_bowling_figures, '/', 2)::integer
where max_number_of_wickets_per_match IS NULL;

UPDATE ipl_stats.bowler_statistics
SET max_number_of_wickets_per_match = split_part(best_bowling_figures, '/', 2)::integer
where max_number_of_wickets_per_match > 5
and player not in ('Alzarri Joseph', 'Adam Zampa');


CREATE TABLE ipl_stats.prediction_bowlers_stats_data AS
select
    player,
    matches,
    innings,
    number_of_overs,
    runs,
    wickets,
    number_of_four_fers,
    number_of_five_fers,
    max_number_of_wickets_per_match,
    number_of_balls_bowled,
    (cast(runs as FLOAT)/wickets) as average,
    (cast(number_of_balls_bowled as float)/wickets) as strike_rate
FROM (
    SELECT
        player,
        sum(matches) as matches,
        sum(innings) as innings,
        sum(number_of_overs) as number_of_overs,
        sum(runs) as runs,
        sum(wickets) as wickets,
        max(number_of_four_fers) as number_of_four_fers,
        max(number_of_five_fers) as number_of_five_fers,
        max(max_number_of_wickets_per_match) as max_number_of_wickets_per_match,
        sum(number_of_balls_bowled) as number_of_balls_bowled
    FROM ipl_stats.bowler_statistics
    group by player
    order by player
) a1;
