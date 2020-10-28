-- 1. Which English wikipedia article got the most traffic on October 20?

-- Create local directory to store files
-- wget all 24 files
-- gunzip all 24 files
-- Create new directory in hdfs to store files
-- Push all files to hdfs

-- Create new table for all pageviews durin October
CREATE TABLE ALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;

-- Load in all data
LOAD DATA INPATH '/user/vern/pageviews/pageviews-20201020-{00..23}0000' INTO TABLE ALLPAGEVIEWS;

-- Create new table with only DOMAIN_CODE = 'en'
CREATE TABLE AllENPAGEVIEWS
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM ALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en';

-- Create query to find PAGE_TITLE, SUM(COUNT_VIEWS) and return top 10 in DESC order for viewing purpose
SELECT PAGE_TITLE, SUM(COUNT_VIEWS) 
FROM ALLENPAGEVIEWS 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC
LIMIT 10;

+-------------------+----------+
|    page_title     |   _c1    |
+-------------------+----------+
| Main_Page         | 2726387  |
| Special:Search    | 910309   |
| Bible             | 148726   |
| -                 | 124890   |
| Jeffrey_Toobin    | 116724   |
| Microsoft_Office  | 71825    |
| Deaths_in_2020    | 62082    |
| F5_Networks       | 61049    |
| Robert_Redford    | 56808    |
| Jeff_Bridges      | 48696    |
+-------------------+----------+

-- REVIEW WITH CONDITION 'en%'

-- FINISHED #1


-- 2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article?

-- Create local directory to store file
-- wget clickstream
-- gunzip
-- Create new directory in hdfs to store file
-- Push all file to hdfs

-- Create new db for clickstream
CREATE DATABASE CLICKSTREAM_DB;

-- Create new table for clickstream
CREATE TABLE CLICKSTREAM
(PREV_ARTICLE STRING, CURR_ARTICLE STRING, TYPE STRING, NUM_OCC INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'

-- Load in data from sotred file on hdfs
LOAD DATA INPATH '/user/vern/clickstream/clickstream-enwiki-2020-09.tsv' INTO TABLE CLICKSTREAM

-- Create new table with only 'interal' links
CREATE TABLE INTERNALCLICKSTREAM
AS SELECT PREV_ARTICLE, CURR_ARTICLE, TYPE, NUM_OCC
FROM CLICKSTREAM
WHERE TYPE = 'link';

-- Create query to find PAGE_TITLE, SUM(LINK) and return top 10 in desc order for viewing purposes
SELECT PREV_ARTICLE, SUM(NUM_OCC)
FROM INTERNALCLICKSTREAM
GROUP BY PREV_ARTICLE
ORDER BY SUM(NUM_OCC) DESC
LIMIT 10;

+----------------------------+----------+
|        prev_article        |   _c1    |
+----------------------------+----------+
| Ruth_Bader_Ginsburg        | 2489227  |
| Main_Page                  | 2379287  |
| Cobra_Kai                  | 2241751  |
| The_Boys_(2019_TV_series)  | 2006351  |
| Mulan_(2020_film)          | 1749519  |
| Ratched_(TV_series)        | 1668477  |
| Deaths_in_2020             | 1595715  |
| Amy_Coney_Barrett          | 1413345  |
| Tenet_(film)               | 1386086  |
| Enola_Holmes_(film)        | 1356311  |
+----------------------------+----------+

-- FINISHED #2

