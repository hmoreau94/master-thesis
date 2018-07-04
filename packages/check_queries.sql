/*
AUTHOR:
    Hugo Moreau - hugo.moreau@epfl.ch
    Msc in Communication Systems
    Minor in Management and Technological Entrepreneurship

Queries to check the content of the tables
*/
SELECT * FROM LOG_TABLE ORDER BY LOG_DATE ;

select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.vector@DMT2DMP;
select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.DAILY_AVG_DIFFS@DMT2DMP order by day_0; 
select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.DAILY_AVG_DAY_0@DMT2DMP order by day_0;
select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.VIA_MACS@DMT2DMP order by day_0;

select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.vector_five_days_II order by day_0;
select /*+ Parallel(16) */ distinct(day_0) from HUMOREAU.VIA_MACS order by day_0;