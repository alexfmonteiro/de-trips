-- What regions has the "cheap_mobile" datasource appeared in?
select distinct region, count(datasource) as cheap_mobile_appearances
  from trips
 where datasource = 'cheap_mobile'
 group by region
 order by 2 desc;