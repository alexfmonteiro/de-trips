SELECT region, daily_avg, weekly_avg, monthly_avg
  FROM (SELECT region, avg(count) monthly_avg
          FROM (SELECT region, count(*)
                  FROM trips
                 GROUP BY region, date_trunc('month', datetime)) s
         GROUP BY 1) monthly

JOIN (SELECT region, avg(count) weekly_avg
        FROM (SELECT region, count(*)
                FROM trips
               GROUP BY region, date_trunc('week', datetime)) s
       GROUP BY 1) weekly USING(region)

JOIN (SELECT region, avg(count) daily_avg
        FROM (SELECT region, count(*)
                FROM trips
               GROUP BY region, date_trunc('day', datetime)) s
       GROUP BY 1) daily USING(region);
