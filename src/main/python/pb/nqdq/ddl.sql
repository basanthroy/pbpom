CREATE TABLE `PB_QUEUE` (
  `request_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_ts` varchar(20) DEFAULT NULL,
  `strategy_id` int(11) DEFAULT NULL,
  `algo_id` int(11) DEFAULT NULL,
  `event_type` ENUM ('new', 'changed'),
  `update_ts` varchar(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`request_id`)
);

insert into PB_QUEUE select * from test_arte.JOIN_REQUESTS;

CREATE TABLE `PB_STRATEGY_ALGO` (
  `strategy_id` int(11) NOT NULL DEFAULT '0',
  `algo_id` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`strategy_id`,`algo_id`)
);

insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(542378, 901);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(552211, 1);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(584795, 2);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(595025, 2);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(595026, 2);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) values(595027, 2);
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) select strategy_id, 1 from PB_STRATEGIES_CURRENT;
insert into PB_STRATEGY_ALGO(strategy_id, algo_id) select strategy_id, 2 from PB_STRATEGIES_CURRENT;


CREATE TABLE `PB_STRATEGIES_CURRENT` (
  `strategy_id` int(11) NOT NULL,
  `baseline_bid` decimal(10,4) DEFAULT NULL,
  `max_bid` decimal(10,4) DEFAULT NULL,
  PRIMARY KEY (`strategy_id`)
);

CREATE TABLE `PB_STRATEGIES_PREVIOUS` (
  `strategy_id` int(11) NOT NULL,
  `baseline_bid` decimal(10,4) DEFAULT NULL,
  `max_bid` decimal(10,4) DEFAULT NULL,
  PRIMARY KEY (`strategy_id`)
);


-- Queries
------------
-- Mysql
select * from PB_STRATEGIES_CURRENT pbstrat JOIN PB_STRATEGY_ALGO pbstalgo ON (pbstrat.strategy_id = pbstalgo.strategy_id);

select psa.strategy_id, psc.baseline_bid, psc.max_bid from PB_STRATEGY_ALGO psa JOIN PB_STRATEGIES_CURRENT psc ON (psa.strategy_id = psc.strategy_id) where psa.algo_id = 2;


-- hive
CREATE  TABLE `broy.tmp_pb_dimension_predictions`(
  `strategy_id` int,
  `dimension_value_structs` struct<dimension_type_id:int,dimension_value:struct<first:string,last:string,unit:string>>,
  `probability_score` double)
PARTITIONED BY (
  `algo_id` int)

-- not sure this works
INSERT INTO TABLE tmp_pb_dimension_predictions SELECT stack(1 , 552211, '{"dimension_type_id":1,"dimension_value":{"first":"macmillandictionary.com","last":"","unit":""}}', 5.747787101965744E-44 , 2) AS (strategy_id, dimension_value_structs, probability_score, algo_id) FROM arte_dimension_predictions;

-- to insert
insert into table tmp_pb_dimension_predictions PARTITION(algo_id=1) select strategy_id, dimension_value_structs, probability_score from adpred.pb_dimension_predictions;
insert into table tmp_pb_dimension_predictions PARTITION(algo_id=1) select 552211 as strategy_id, dimension_value_structs, probability_score from adpred.pb_dimension_predictions;

-- to delete
insert overwrite table tmp_pb_dimension_predictions PARTITION(algo_id=1)  select strategy_id, dimension_value_structs, probability_score from adpred.pb_dimension_predictions where 1 = 2;

-- load data
load data inpath '/user/broy/tmp_pb_dimension_predictions.txt' into table tmp_pb_dimension_predictions PARTITION(algo_id=1);
Loading data to table broy.tmp_pb_dimension_predictions partition (algo_id=1)



-- Data population
insert into PB_STRATEGIES_CURRENT (strategy_id, baseline_bid, max_bid) values(10001, 1.01, 2.02);
delete from PB_STRATEGIES_PREVIOUS where strategy_id = 536482;
update PB_STRATEGIES_CURRENT set baseline_bid = baseline_bid * 1.05 where strategy_id = 536258;
update PB_STRATEGIES_CURRENT set baseline_bid = max_bid * 0.95 where strategy_id = 535597;
insert into PB_STRATEGIES_PREVIOUS (strategy_id, baseline_bid, max_bid) values(20002, 1.02, 2.03);


-- new strategies
select strategy_id from PB_STRATEGIES_CURRENT where strategy_id NOT IN (select strategy_id from PB_STRATEGIES_PREVIOUS);
-- deleted strategies
select strategy_id from PB_STRATEGIES_PREVIOUS where strategy_id NOT IN (select strategy_id from PB_STRATEGIES_CURRENT);
-- changed strategies
select curr.strategy_id from PB_STRATEGIES_CURRENT curr JOIN PB_STRATEGIES_PREVIOUS prev on (curr.strategy_id = prev.strategy_id) where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid));
select curr.strategy_id, st_alg.algo_id from PB_STRATEGIES_CURRENT curr JOIN PB_STRATEGIES_PREVIOUS prev on (curr.strategy_id = prev.strategy_id) JOIN PB_STRATEGY_ALGO st_alg on (curr.strategy_id = st_alg.strategy_id) where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid));


-- Unified diff
select distinct algos.algo_id from (select st_alg.algo_id from PB_STRATEGIES_CURRENT curr JOIN PB_STRATEGIES_PREVIOUS prev on (curr.strategy_id = prev.strategy_id) JOIN PB_STRATEGY_ALGO st_alg on (curr.strategy_id = st_alg.strategy_id) where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid)) UNION ALL select st_alg.algo_id from PB_STRATEGIES_CURRENT curr JOIN PB_STRATEGY_ALGO st_alg ON (curr.strategy_id = st_alg.strategy_id) where curr.strategy_id NOT IN (select strategy_id from PB_STRATEGIES_PREVIOUS)) algos;

select distinct algos.algo_id
from (select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGIES_PREVIOUS prev
      on (curr.strategy_id = prev.strategy_id)
      JOIN PB_STRATEGY_ALGO st_alg
      on (curr.strategy_id = st_alg.strategy_id)
      where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid))
      UNION ALL
      select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGY_ALGO st_alg
      ON (curr.strategy_id = st_alg.strategy_id)
      where curr.strategy_id NOT IN (select strategy_id from PB_STRATEGIES_PREVIOUS)) algos;


select distinct algos.algo_id,
      DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as create_ts,
      DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as update_ts,
      'pending' as status
from (select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGIES_PREVIOUS prev
      on (curr.strategy_id = prev.strategy_id)
      JOIN PB_STRATEGY_ALGO st_alg
      on (curr.strategy_id = st_alg.strategy_id)
      where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid))
      UNION ALL
      select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGY_ALGO st_alg
      ON (curr.strategy_id = st_alg.strategy_id)
      where curr.strategy_id NOT IN (select strategy_id from PB_STRATEGIES_PREVIOUS)) algos;

insert into PB_QUEUE(algo_id, create_ts, update_ts, status)
select distinct algos.algo_id,
      DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as create_ts,
      DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as update_ts,
      'pending' as status
from (select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGIES_PREVIOUS prev
      on (curr.strategy_id = prev.strategy_id)
      JOIN PB_STRATEGY_ALGO st_alg
      on (curr.strategy_id = st_alg.strategy_id)
      where ((curr.baseline_bid != prev.baseline_bid) or (curr.max_bid != prev.max_bid))
      UNION ALL
      select st_alg.algo_id from PB_STRATEGIES_CURRENT curr
      JOIN PB_STRATEGY_ALGO st_alg
      ON (curr.strategy_id = st_alg.strategy_id)
      where curr.strategy_id NOT IN (select strategy_id from PB_STRATEGIES_PREVIOUS)) algos;


CREATE  TABLE `broy.pb_dimension_predictions`(
  `strategy_id` int,
  `dimension_value_structs` struct<dimension_type_id:int,dimension_value:struct<first:string,last:string,unit:string>>,
  `probability_score` double)
PARTITIONED BY (
  `algo_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



INSERT OVERWRITE TABLE broy.pb_dimension_predictions_tmp PARTITION (algo_id =1)
SELECT strats.strategy_id,
     NAMED_STRUCT('dimension_type_id', 1, 'dimension_value', NAMED_STRUCT('first', CAST(preds.domain_name AS STRING), 'last', "", 'unit', "")    ) AS dimension_value_structs,
     preds.percentage_recommendations AS probability_score
FROM (SELECT strategy_id, line_id
    FROM radiumone_master.strategies
    WHERE bidding_engine_id = 2
      and strategy_id in (542378, 552211, 584795, 595025, 595026, 595027)) strats
   JOIN radiumone_master.linez l
  ON strats.line_id = l.line_id
   JOIN dchugh.line_recommender_cumsum preds
  ON l.line_id = preds.line_id;



CREATE  TABLE `programmable_bid_domainbidderinputfield`(
  `strategy_id` int,
  `bid_list_id` int,
  `min_bid` double,
  `max_bid` double,
  `algorithm_id` int,
  `dimension_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode1.dw.sc.gwallet.com:8020/user/hive/warehouse/broy.db/programmable_bid_domainbidderinputfield'


2016/05/25 and 26
-----------------
CREATE  TABLE `pb_dimension_predictions`(
  `strategy_id` int,
  `dimension_value_structs` struct<dimension_type_id:int,dimension_value:struct<first:string,last:string,unit:string>>,
  `probability_score` double)
PARTITIONED BY (
  `algo_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'


CREATE TABLE `PB_QUEUE` (
  `request_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_ts` varchar(20) DEFAULT NULL,
  `strategy_id` int(11) DEFAULT NULL,
  `algo_id` int(11) DEFAULT NULL,
  `dimension_type` varchar(255) DEFAULT NULL,
  `event_type` enum('new','changed') DEFAULT NULL,
  `update_ts` varchar(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`request_id`)
)

CREATE  TABLE `programmable_bid_domainbidderinputfield`(
  `strategy_id` int,
  `bid_list_id` int,
  `line_id` int,
  `unique_count` int,
  `email` string,
  `min_bid` double,
  `max_bid` double,
  `algorithm_id` int,
  `dimension_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'



CREATE EXTERNAL TABLE `pb_output_log`(
  `strategy_id` bigint COMMENT 'from deserializer',
  `dimension_structs` array<struct<dimension_type_id:bigint,dimension_value:string>> COMMENT 'from deserializer',
  `probability_score` double COMMENT 'from deserializer',
  `min_bid` double COMMENT 'from deserializer',
  `max_bid` double COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `multiplier` double COMMENT 'from deserializer')
PARTITIONED BY (
  `algo_id` int,
  `dt` int)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode1.dw.sc.gwallet.com:8020/user/arteu/programmablebidder/prod/log'


06/09 - PB V1.1
------------------
CREATE EXTERNAL TABLE `pb_output_log_test`(
  `strategy_id` bigint COMMENT 'from deserializer',
  `dimension_structs` array<struct<dimension_type_id:bigint,dimension_value:string>> COMMENT 'from deserializer',
  `probability_score` double COMMENT 'from deserializer',
  `min_bid` double COMMENT 'from deserializer',
  `max_bid` double COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `multiplier` double COMMENT 'from deserializer')
PARTITIONED BY (
  `algo_id` int,
  `dt` int)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode1.dw.sc.gwallet.com:8020/user/arteu/programmablebidder/test/log'




CREATE  TABLE `pb_dimension_predictions_test`(
  `strategy_id` int,
  `dimension_value_structs` struct<dimension_type_id:int,dimension_value:struct<first:string,last:string,unit:string>>,
  `probability_score` double)
PARTITIONED BY (
  `algo_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode1.dw.sc.gwallet.com:8020/user/hive/warehouse/adpred.db/pb_dimension_predictions_test'




CREATE TABLE `programmable_bid_domainbidderinputfield`(
  `strategy_id` int,
  `bid_list_id` int,
  `line_id` int,
  `unique_count` int,
  `email` string,
  `min_bid` double,
  `max_bid` double,
  `algorithm_id` int,
  `dimension_id` int,
  `target_ecpm` double)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://namenode1.dw.sc.gwallet.com:8020/user/hive/warehouse/adpred.db/programmable_bid_domainbidderinputfield'



select domain.strategy_id,
                     domain.bid_list_id,
                     domain.line_id,
                     COALESCE(uniques.number_of_unique_users, -1) as uniques_count,
                     login.email,
                     domain.min_bid,
                     domain.max_bid,
                     COALESCE(domain.algorithm_id, -1),
                     1 as dimension_id,
                     domain.target_cpm
                     from programmable_bid_domainbidderinputfield domain
                     LEFT JOIN programmable_bid_uniquesselection uniques
                     ON domain.uniques_id = uniques.id
                     JOIN login_optimizer login
                     ON domain.user_id = login.id

-- ###############################
-- POM PB pipeline - 2016/11/15
-- ###############################
ALTER TABLE pb_dimension_predictions RENAME TO pom_pb_dimension_predictions;
ALTER TABLE programmable_bid_domainbidderinputfield RENAME TO pom_programmable_bid_domainbidderinputfield;



