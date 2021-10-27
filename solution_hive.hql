use student6;

create external table scopus_info_ex (
dc_identifier string,
publication_name string,
cover_date string,
cited_count int,
article_type string,
sub_type_description string,
open_access_flag string,
dc_title string,
display_year int)
row format delimited
fields terminated by '\t'
stored as textfile
tblproperties ("skip.header.line.count"="1");

load data inpath '/user/student6/publication.txt' overwrite into table scopus_info_ex;

create table scopus_info_in ( 
dc_identifier string,
publication_name string,
cover_date string,
cited_count int,
article_type string,
sub_type_description string,
open_access_flag string,
dc_title string,
display_year int)
row format delimited
fields terminated by '\t'
stored as textfile
tblproperties ("skip.header.line.count"="1");

load data inpath '/user/student6/publication.txt' overwrite into table scopus_info_in;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=50;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;

create table scopus_info_p (
dc_identifier string, 
publication_name string, 
cover_date string, 
cited_count int, 
sub_type_description string, 
open_access_flag string, 
dc_title string, 
display_year int)  
partitioned by (articletype string) 
row format delimited 
fields terminated by '\t'
stored as textfile 
tblproperties ("skip.header.line.count"="1");

insert overwrite table scopus_info_p partition (articletype) 
select dc_identifier, publication_name, cover_date,  
cited_count, sub_type_description, open_access_flag, dc_title, 
display_year, article_type from scopus_info_ex;


show partitions scopus_info_p;


create table scopus_info_c (
dc_identifier string,
publication_name string, 
cover_date string, 
cited_count int, 
sub_type_description string, 
open_access_flag string, 
dc_title string, 
display_year int)
partitioned by (article_type string)
clustered by (display_year) into 5 buckets
row format delimited
fields terminated by '\t'
stored as textfile
tblproperties ("skip.header.line.count"="1");

insert overwrite table scopus_info_c partition (article_type) 
select dc_identifier, publication_name, cover_date,  
cited_count, sub_type_description, open_access_flag, dc_title, 
display_year, article_type from scopus_info_ex;


show partitions scopus_info_c;

select article_type, display_year, count(*) as Total
from scopus_info_ex where display_year 
BETWEEN '2018' AND '2020' 
group by article_type, display_year;

select article_type, display_year, count(*) as Total
from scopus_info_in where display_year 
BETWEEN '2018' AND '2020' 
group by article_type, display_year;

select articletype, display_year, count(*) as Total
from scopus_info_p where display_year 
BETWEEN '2018' AND '2020'  
group by articletype, display_year;

select article_type, display_year, count(*) as Total 
from scopus_info_c where display_year 
BETWEEN '2018' AND '2020' 
group by article_type, display_year;

select article_type, display_year, count(*) as total_publication from 
scopus_info_ex where dc_title like '%big data%' or dc_title like '%data analytics%' 
and display_year between 2018 and 2020 group by article_type, display_year;

select article_type, display_year, count(*) as total_publication from 
scopus_info_in where dc_title like '%big data%' or dc_title like '%data analytics%' 
and display_year between 2018 and 2020 group by article_type, display_year;

select articletype, display_year, count(*) as total_publication from 
scopus_info_p where dc_title like '%big data%' or dc_title like '%data analytics%' 
and display_year between 2018 and 2020 group by articletype, display_year;

select article_type, display_year, count(*) as total_publication from 
scopus_info_c where dc_title like '%big data%' or dc_title like '%data analytics%' 
and display_year between 2018 and 2020 group by article_type, display_year;

select publication_name, display_year, cited_count from scopus_info_ex 
where display_year between '2018' and '2020' group by publication_name, display_year, cited_count
order by cited_count desc limit 5;

select publication_name, display_year, cited_count from scopus_info_in 
where display_year between '2018' and '2020' group by publication_name, display_year, cited_count
order by cited_count desc limit 5;

select publication_name, display_year, cited_count from scopus_info_p 
where display_year between '2018' and '2020' group by publication_name, display_year, cited_count
order by cited_count desc limit 5;

select publication_name, display_year, cited_count from scopus_info_c 
where display_year between '2018' and '2020' group by publication_name, display_year, cited_count
order by cited_count desc limit 5;

