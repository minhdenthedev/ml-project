INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/action' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.action = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/adventure' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.adventure = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/animation' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.animation = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/children' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.children = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/comedy' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.comedy = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/crime' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.crime = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/documentary' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.documentary = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/drama' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.drama = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/fantasy' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.fantasy = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/film_noir' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.film_noir = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/horror' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.horror = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/musical' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.musical = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/mystery' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.mystery = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/romance' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.romance = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/scifi' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.scifi = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/thriller' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.thriller = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/war' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.war = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;

INSERT OVERWRITE LOCAL DIRECTORY '/home/minh/Work/ml-project/hive_results/genre_pop_time/western' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select count(*), weekofyear(from_unixtime(unixtime)) as week, from_unixtime(unixtime, 'yyyy') as year from u_data join u_item on u_data.movie_id = u_item.movie_id where u_item.western = 1 group by weekofyear(from_unixtime(unixtime)), from_unixtime(unixtime, 'yyyy') order by year, week;
