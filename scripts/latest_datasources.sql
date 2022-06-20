-- From the two most commonly appearing regions, which is the latest datasource?
select region, datasource as latest_datasource
  from (select region, datetime, datasource,
	           rank() OVER (PARTITION BY region order by datetime desc),
	           count(*) OVER (PARTITION BY region) cnt
          from trips
         order by rank) as t
 where rank = 1
 order by cnt desc
 limit 2;