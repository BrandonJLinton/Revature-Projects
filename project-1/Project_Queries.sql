-- 1. Which English wikipedia article got the most traffic on October 20?

-- Create local directory to store files
-- wget all 24 files
-- gunzip all 24 files
-- Create new directory in hdfs to store files
-- Push all files to hdfs

-- Create new table for all pageviews on October 20th
CREATE TABLE ALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;

-- Load in all data
LOAD DATA INPATH '/user/vern/pageviews/' INTO TABLE ALLPAGEVIEWS;


-- Create new table with only DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m'
CREATE TABLE ENALLPAGEVIEWS
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM ALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m';

-- Create query to find PAGE_TITLE, SUM(COUNT_VIEWS) and return top 10 in DESC order for viewing purpose
CREATE TABLE ENTOTALPAGEVIEWS
AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS) AS TOTAL_PAGE_VIEWS
FROM ENALLPAGEVIEWS 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC;

SELECT * FROM ENTOTALPAGEVIEWS LIMIT 10;

+------------------------------+------------------------------------+
| entotalpageviews.page_title  | entotalpageviews.total_page_views  |
+------------------------------+------------------------------------+
| Main_Page                    | 5961008                            |
| Special:Search               | 1476831                            |
| -                            | 544714                             |
| Jeffrey_Toobin               | 321459                             |
| C._Rajagopalachari           | 210558                             |
| The_Haunting_of_Bly_Manor    | 185139                             |
| Robert_Redford               | 178779                             |
| Jeff_Bridges                 | 159163                             |
| Bible                        | 151484                             |
| Chicago_Seven                | 149966                             |
+------------------------------+------------------------------------+


-- REVIEW WITH CONDITION 'en%'

-- FINISHED #1


-- 2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article?
-- CURR_ARTICLE => PAGE_TITLE

-- Create local directory to store file
-- wget clickstream
-- gunzip
-- Create new directory in hdfs to store file
-- Push all file to hdfs

-- Create new table for clickstream
CREATE TABLE ALLCLICKSTREAM
(PREV_ARTICLE STRING, CURR_ARTICLE STRING, TYPE STRING, NUM_OCC INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'

CREATE TABLE SEPTALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;

-- Load in data from sotred file on hdfs
LOAD DATA INPATH '/user/vern/clickstream/clickstream-enwiki-2020-09.tsv' INTO TABLE CLICKSTREAM

LOAD DATA INPATH '/user/vern/septpageviews' INTO TABLE SEPTALLPAGEVIEWS

CREATE TABLE SEPTENALLPAGEVIEWS
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM SEPTALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m';

SELECT PAGE_TITLE, SUM(COUNT_VIEWS)
FROM SEPTENALLPAGEVIEWS 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC
LIMIT 10;

CREATE TABLE SEPTTOTALPAGEVIEWS
AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS) AS TOTAL_PAGE_VIEWS
FROM SEPTENALLPAGEVIEWS 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC;

-- Create new table with only 'interal' links
CREATE TABLE ALLINTERNALCLICKSTREAM
AS SELECT PREV_ARTICLE, CURR_ARTICLE, TYPE, NUM_OCC
FROM ALLCLICKSTREAM
WHERE TYPE = 'link';

-- Create query to find PAGE_TITLE, SUM(LINK)
CREATE TABLE INTERNALTOTALCLICKSTREAM
AS SELECT PREV_ARTICLE, SUM(NUM_OCC) AS INTERNAL_LINKS
FROM ALLINTERNALCLICKSTREAM
GROUP BY PREV_ARTICLE
ORDER BY SUM(NUM_OCC) DESC;

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

-- Infer that views are relatively equal per day, multiple October 20th views by 30
-- NO FILTER LEAVES ME WITH FRACTIONS THAT ARE RIDULOUSLY BIG
-- OVER 500000 STILL LEAVES ME WITH FRACTIONS OVER 100
-- OVER 1000000 LEAVES ME WITH FRACTIONS THAT ARE MANAGEABLE
SELECT INTERNALTOTALCLICKSTREAM.PREV_ARTICLE AS ARTICLE, SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS AS TOTAL_PAGE_VIEWS, INTERNALTOTALCLICKSTREAM.INTERNAL_LINKS AS NUM_LINKS, ROUND(INTERNALTOTALCLICKSTREAM.INTERNAL_LINKS / (SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS LARGEST_FRACTION
FROM INTERNALTOTALCLICKSTREAM
INNER JOIN SEPTTOTALPAGEVIEWS ON INTERNALTOTALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
WHERE SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS > 1000000
ORDER BY LARGEST_FRACTION DESC
LIMIT 10;


-- FINISHED #2

-- 3. What series of wikipedia articles, starting with [Hotel California](https://en.wikipedia.org/wiki/Hotel_California), keeps the largest fraction of its readers clicking on internal links?  This is similar to (2), but you should continue the analysis past the first article.

-- Find the article (starting with Hotel California) that keeps the largest fraction of its readers clicking on internal links
-- We will reuse a previous table called ALLINTERNALCLICKSTREAM which is all EN items that have already been parsed to only show 'internally' linked articles
CREATE TABLE LARGESTLINKED_ONE
    AS SELECT PREV_ARTICLE, CURR_ARTICLE, NUM_OCC, ROUND((ALLINTERNALCLICKSTREAM.NUM_OCC / SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS FRACTION_CLICKED
    FROM ALLINTERNALCLICKSTREAM
    INNER JOIN SEPTTOTALPAGEVIEWS ON ALLINTERNALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
    WHERE PREV_ARTICLE = 'Hotel_California'
    ORDER BY NUM_OCC DESC
    LIMIT 10;

SELECT * FROM LARGESTLINKED_ONE;
-- This shows that we have 'Hotel_California_(Eagles_album)' at '2222' occurrences as our first largest linked article


-- Find the largest linked based on the top result of the first query
CREATE TABLE LARGESTLINKED_TWO
    AS SELECT PREV_ARTICLE, CURR_ARTICLE, NUM_OCC, ROUND((ALLINTERNALCLICKSTREAM.NUM_OCC / SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS FRACTION_CLICKED
    FROM ALLINTERNALCLICKSTREAM
    INNER JOIN SEPTTOTALPAGEVIEWS ON ALLINTERNALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
    WHERE PREV_ARTICLE IN 
    (
        SELECT CURR_ARTICLE
        FROM LARGESTLINKED_ONE
        LIMIT 1
    )
    ORDER BY NUM_OCC DESC
    LIMIT 10;
    
SELECT * FROM LARGESTLINKED_TWO;
-- We are returned with the result of 'The_Long_Run(album)' with '2127' and 'Hotel_California' '2010'. Since our referrer is present that is our largest, but since we don't want to go back to the referrer
-- we are now following 'The_Long_Run(album)'.

-- Find the largest linked based on the top result of the SECOND query
CREATE TABLE LARGESTLINKED_THREE
    AS SELECT PREV_ARTICLE, CURR_ARTICLE, NUM_OCC, ROUND((ALLINTERNALCLICKSTREAM.NUM_OCC / SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS FRACTION_CLICKED
    FROM ALLINTERNALCLICKSTREAM
    INNER JOIN SEPTTOTALPAGEVIEWS ON ALLINTERNALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
    WHERE PREV_ARTICLE IN 
    (
        SELECT CURR_ARTICLE
        FROM LARGESTLINKED_TWO
        LIMIT 1
    )
    ORDER BY NUM_OCC DESC
    LIMIT 10;

SELECT * FROM LARGESTLINKED_THREE;
-- This shows that we have 'Eagles_Live' at '1322' occurrences

-- Find the largest linked based on the top result of the THIRD query
CREATE TABLE LARGESTLINKED_FOUR
    AS SELECT PREV_ARTICLE, CURR_ARTICLE, NUM_OCC, ROUND((ALLINTERNALCLICKSTREAM.NUM_OCC / SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS FRACTION_CLICKED
    FROM ALLINTERNALCLICKSTREAM
    INNER JOIN SEPTTOTALPAGEVIEWS ON ALLINTERNALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
    WHERE PREV_ARTICLE IN 
    (
        SELECT CURR_ARTICLE
        FROM LARGESTLINKED_THREE
        LIMIT 1
    )
    ORDER BY NUM_OCC DESC
    LIMIT 10;
    
SELECT * FROM LARGESTLINKED_FOUR

CREATE TABLE LARGESTLINKED_FIVE
    AS SELECT PREV_ARTICLE, CURR_ARTICLE, NUM_OCC, ROUND((ALLINTERNALCLICKSTREAM.NUM_OCC / SEPTTOTALPAGEVIEWS.TOTAL_PAGE_VIEWS) * 100, 2) AS FRACTION_CLICKED
    FROM ALLINTERNALCLICKSTREAM
    INNER JOIN SEPTTOTALPAGEVIEWS ON ALLINTERNALCLICKSTREAM.PREV_ARTICLE = SEPTTOTALPAGEVIEWS.PAGE_TITLE
    WHERE PREV_ARTICLE IN 
    (
        SELECT CURR_ARTICLE
        FROM LARGESTLINKED_FOUR
        LIMIT 1
    )
    ORDER BY NUM_OCC DESC
    LIMIT 10;
    
SELECT * FROM LARGESTLINKED_FIVE;


-- 4. Find an example of an English wikipedia article that is relatively more popular in the UK.  Find the same for the US and Australia.

-- 2017 indicates very strongly that daily web use peaks between 9am and midday, falling off steadily throughout the day with a modest levelling off between 7pm and 10pm, and then collapses to a base at 4am.

-- Data is in UTC timezone which corresponds to UK
-- Typically, in the UK the peak hours are between 7 and 11 pm

-- Maybe 3 hours (11-2) and 4 hours (7-11) for all zones. I want to keep the time periods even, as it will give innaccurate results if I guess incorrectly.
-- the internet is busiest between 9pm and 11pm around the world in general. People frequent their phones around lunch time as well.

-- US - 5 hours behind UTC
-- 06-09 and 14-18
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{06..09}0000.gz
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{14..18}0000.gz

-- UK - Already stored in UTC
-- 11-14 and 19-23

wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{11..14}0000.gz
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{19..23}0000.gz

-- Australia
-- 22-01 and 06-10
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{00..01}0000.gz
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{22..23}0000.gz
wget https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{06..10}0000.gz

-- Create hdfs folders - uk aus us
-- Transfer respective files
-- Create tables

CREATE TABLE USALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;
LOAD DATA INPATH '/user/vern/USPageViews' INTO TABLE USALLPAGEVIEWS;

CREATE TABLE UKALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;
LOAD DATA INPATH '/user/vern/UKPageViews' INTO TABLE UKALLPAGEVIEWS;

CREATE TABLE AUSALLPAGEVIEWS
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    '
;
LOAD DATA INPATH '/user/vern/AUSPageViews' INTO TABLE AUSALLPAGEVIEWS;

CREATE TABLE ENALLPAGEVIEWS_US
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM USALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m';

CREATE TABLE ENALLPAGEVIEWS_UK
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM UKALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m';

CREATE TABLE ENALLPAGEVIEWS_AUS
AS SELECT DOMAIN_CODE, PAGE_TITLE, COUNT_VIEWS, TOTAL_RESPONSE_SIZE 
FROM AUSALLPAGEVIEWS 
WHERE DOMAIN_CODE = 'en' OR DOMAIN_CODE = 'en.m';

CREATE TABLE ENTOTALPAGEVIEWS_US
AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS)
FROM ENALLPAGEVIEWS_US 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC
LIMIT 20;

SELECT * FROM ENTOTALPAGEVIEWS_US;

CREATE TABLE ENTOTALPAGEVIEWS_UK
AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS)
FROM ENALLPAGEVIEWS_UK 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC
LIMIT 20;

SELECT * FROM ENTOTALPAGEVIEWS_UK;

CREATE TABLE ENTOTALPAGEVIEWS_AUS
AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS)
FROM ENALLPAGEVIEWS_AUS 
GROUP BY PAGE_TITLE 
ORDER BY SUM(COUNT_VIEWS) DESC
LIMIT 20;

SELECT * FROM ENTOTALPAGEVIEWS_AUS;

-- 5. Analyze how many users will see the average vandalized wikipedia page before the offending edit is reversed

-- wiki_db = enwiki
-- event_entity = revision
-- event_type = create

-- revision_seconds_to_identity_revert
-- page_title


create table ENWIKI_REVISIONS (WIKI_DB STRING, 
                EVENT_ENTITY STRING,
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP STRING,
                EVENT_COMMENT STRING,
                EVENT_USER_ID BIGINT,
                EVENT_USER_TEXT_HISTORICAL STRING,
                EVENT_USER_TEXT STRING,
                EVENT_USER_BLOCKS_HISTORICAL STRING,
                EVENT_USER_BLOCKS ARRAY<STRING>,
                EVENT_USER_GROUPS_HISTORICAL ARRAY<STRING>,
                EVENT_USER_GROUPS ARRAY<STRING>,
                event_user_is_bot_by_historical ARRAY<STRING>,
                event_user_is_bot_by ARRAY<STRING>,
                event_user_is_created_by_self BOOLEAN,
                event_user_is_created_by_system BOOLEAN,
                event_user_is_created_by_peer BOOLEAN,
                event_user_is_anonymous BOOLEAN,
                event_user_registration_timestamp STRING,
                event_user_creation_timestamp STRING,
                event_user_first_edit_timestamp STRING,
                event_user_revision_count BIGINT,
                event_user_seconds_since_previous_revision BIGINT,
                page_id BIGINT,
                page_title_historical STRING,
                page_title STRING,
                page_namespace_historical INT,
                page_namespace_is_content_historical BOOLEAN,
                page_namespace INT,
                page_namespace_is_content BOOLEAN,
                page_is_redirect BOOLEAN,
                page_is_deleted BOOLEAN,
                page_creation_timestamp STRING,
                page_first_edit_timestamp STRING,
                page_revision_count BIGINT,
                page_seconds_since_previous_revision BIGINT,
                user_id BIGINT,
                user_text_historical STRING,
                user_text STRING,
                user_blocks_historical ARRAY<STRING>,
                user_blocks ARRAY<STRING>,
                user_groups_historical ARRAY<STRING>,
                user_groups ARRAY<String>,
                user_is_bot_by_historical ARRAY<STRING>,
                user_is_bot_by Array<STRING>,
                user_is_created_by_self BOOLEAN,
                user_is_created_by_system boolean,
                user_is_created_by_peer BOOLEAN,
                user_is_anonymous boolean,
                user_registration_timestamp String,
                user_creation_timestamp STRING,
                user_first_edit_timestamp STRING,
                revision_id bigint,
                revision_parent_id bigint,
                revision_minor_edit boolean,
                revision_deleted_parts Array<String>,
                revision_deleted_parts_are_suppressed boolean,
                revision_text_bytes bigint,
                revision_text_bytes_diff bigint,
                revision_text_sha1 string,
                revision_content_model string,
                revision_content_format string,
                revision_is_deleted_by_page_deletion boolean,
                revision_deleted_by_page_deletion_timestamp string,
                revision_is_identity_reverted boolean,
                revision_first_identity_reverting_revision_id bigint,
                revision_seconds_to_identity_revert bigint,
                revision_is_identity_revert boolean,
                revision_is_from_before_page_creation boolean,
                revision_tags Array<string>
                )
            ROW FORMAT DELIMITED 
            FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/vern/WikiEdits' INTO TABLE ENWIKI_REVISIONS;

SELECT wiki_db, event_entity, event_type, page_title, revision_seconds_to_identity_revert FROM ENWIKI_REVISIONS LIMIT 100;

CREATE TABLE ENWIKI_RIVISIONS_PARSED
AS SELECT wiki_db, event_entity, event_type, page_title, revision_seconds_to_identity_revert 
FROM ENWIKI_REVISIONS
WHERE NOT revision_seconds_to_identity_revert = 'NULL'
AND revision_seconds_to_identity_revert >= 0
AND event_entity = 'revision'
AND event_type = 'create';

-- CALCULATE AVERAGE NUMBER OF VIEWERS PER PAGE PER SECOND IN SEPTEMBER
CREATE TABLE SEPTTOTALPAGEVIEWS_PER_SECOND
AS SELECT PAGE_TITLE, (TOTAL_PAGE_VIEWS / 2592000) AS AVG_USER_PER_SECOND
FROM SEPTTOTALPAGEVIEWS 
;

-- PAIR UP EACH ARTICLE WITH SEPTTOTALPAGEVIEWS_PER_SECOND 
CREATE TABLE ENWIKI_REVISIONS_MERGED
AS SELECT ENWIKI_RIVISIONS_PARSED.page_title, ENWIKI_RIVISIONS_PARSED.revision_seconds_to_identity_revert, SEPTTOTALPAGEVIEWS_PER_SECOND.PAGE_TITLE AS MERGED_PAGE_TITLE, SEPTTOTALPAGEVIEWS_PER_SECOND.AVG_USER_PER_SECOND
FROM ENWIKI_RIVISIONS_PARSED
INNER JOIN SEPTTOTALPAGEVIEWS_PER_SECOND
ON ENWIKI_RIVISIONS_PARSED.page_title = SEPTTOTALPAGEVIEWS_PER_SECOND.PAGE_TITLE
ORDER BY ENWIKI_RIVISIONS_PARSED.page_title
;

-- Provides final analysis on average users/revision
SELECT ROUND(AVG(revision_seconds_to_identity_revert * AVG_USER_PER_SECOND), 2) AS AVG_USERS_SEEN
FROM ENWIKI_REVISIONS_MERGED;

-- AVERAGE TIME TO REVERT IN DATASET
SELECT AVG(revision_seconds_to_identity_revert) AS AVG_seconds
FROM ENWIKI_RIVISIONS_PARSED
LIMIT 20;

-- 6. 

-- How many 'vandalisms' occured along with how any total seconds these 'vandlisms' appeared for

-- Using same table as Q5
-- No nulls
-- No negative values
-- Must be a revision
CREATE TABLE ENWIKI_RIVISIONS_PARSED
AS SELECT wiki_db, event_entity, event_type, page_title, revision_seconds_to_identity_revert 
FROM ENWIKI_REVISIONS
WHERE NOT revision_seconds_to_identity_revert = 'NULL'
AND revision_seconds_to_identity_revert >= 0
AND event_entity = 'revision'
AND event_type = 'create';

-- The entire dataset
SELECT COUNT(page_title) AS NUM_ARTICLES, 
SUM(revision_seconds_to_identity_revert) AS TOTAL_SECONDS_VAN, 
ROUND(AVG(revision_seconds_to_identity_revert), 2)  AS AVG_TOTAL_SECONDS_VAN
FROM ENWIKI_RIVISIONS_PARSED;

-- Not vandalised for more than 7 days
SELECT COUNT(page_title) AS NUM_ARTICLES, 
SUM(revision_seconds_to_identity_revert) AS TOTAL_SECONDS_VAN, 
ROUND(AVG(revision_seconds_to_identity_revert), 2)  AS AVG_TOTAL_SECONDS_VAN
FROM ENWIKI_RIVISIONS_PARSED
WHERE revision_seconds_to_identity_revert < 604800;