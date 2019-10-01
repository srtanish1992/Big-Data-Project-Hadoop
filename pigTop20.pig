ratings = load '/Project/Input/pig_review.csv' using PigStorage(',') as (businessid:chararray,rating:float);
group_data = GROUP ratings by businessid;
foreach_data = FOREACH group_data GENERATE group,AVG(ratings.rating);
order_by_data = ORDER foreach_data BY $1 DESC;
limit_data = LIMIT order_by_data 20; 
STORE limit_data INTO '/Project/PigOutput/' USING PigStorage (',');
