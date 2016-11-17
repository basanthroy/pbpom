__author__ = 'broy'

import logging
import time

import pyhs2
from pb.config import pb_properties

import epoch


class dq_new_class:

    processing_start_time = 0

    def insert_into_pb_dim_pred(self, strat_algos):
        logging.info('')
        logging.info(" Inserting data into hive ...")

        insert_statement = """
        INSERT OVERWRITE TABLE {}.pom_pb_dimension_predictions PARTITION (algo_id)
        SELECT DISTINCT *
        FROM
          (SELECT strategy_id,
                  dimension_value_structs,
                  probability_score,
                  algo_id
           FROM
             (SELECT opti.strategy_id,
                     row_number() over (partition BY opti.strategy_id
                                        ORDER BY preds.cumulative_users ASC) AS row_n,
                     NAMED_STRUCT('dimension_type_id', 1, 'dimension_value', NAMED_STRUCT('first', CAST(sd.site_domain_id AS STRING), 'last', "", 'unit', "")) AS dimension_value_structs,
                     preds.percentage_recommendations AS probability_score,
                     opti.algorithm_id AS algo_id
              FROM {}.pom_programmable_bid_domainbidderinputfield opti
              JOIN radiumone_master.linez l ON opti.line_id = l.line_id
              JOIN dchugh.line_recommender_cumsum preds ON l.line_id = preds.line_id
              JOIN radiumone_master.SITE_DOMAINS sd ON (preds.domain_name = sd.NAME)
              ) first_domain
           WHERE first_domain.row_n = 1
           UNION ALL SELECT opti.strategy_id,
                            NAMED_STRUCT('dimension_type_id', 1, 'dimension_value', NAMED_STRUCT('first', CAST(sd.site_domain_id AS STRING), 'last', "", 'unit', "")) AS dimension_value_structs,
                            preds.percentage_recommendations AS probability_score,
                            opti.algorithm_id AS algo_id
           FROM {}.pom_programmable_bid_domainbidderinputfield opti
           JOIN radiumone_master.linez l ON opti.line_id = l.line_id
           JOIN dchugh.line_recommender_cumsum preds ON l.line_id = preds.line_id
           JOIN radiumone_master.SITE_DOMAINS sd ON (preds.domain_name = sd.NAME)
           WHERE (preds.cumulative_users <= opti.unique_count or (opti.unique_count = -1)) ) full_query
        ORDER BY strategy_id
        """.format(pb_properties.hive_db, pb_properties.hive_db, pb_properties.hive_db)

        with pyhs2.connect(host=pb_properties.hive_server_host,
                           user=pb_properties.hive_server_user,
                           password=pb_properties.hive_server_password,
                           port=pb_properties.hive_server_port,
                           authMechanism=pb_properties.hive_server_auth) as conn:
            with conn.cursor() as cur:
                logging.info('')
                logging.info(
                    ' In host, jobserver, executing the hql statement: ' +
                    insert_statement)

                hive_settings_dyn_partition_enable = "set hive.exec.dynamic.partition=true"
                hive_settings_dyn_partition_mode = "set hive.exec.dynamic.partition.mode=nonstrict"

                cur.execute(hive_settings_dyn_partition_enable)
                cur.execute(hive_settings_dyn_partition_mode)

                cur.execute(insert_statement)

    def main(self, strat_algos):

        logging.info(
            ' About to invoke dq_new.main ..., elapsed time = {} '
                .format(epoch.get_time_delta(self.processing_start_time)))
        component_start_time = int(time.time())

        self.insert_into_pb_dim_pred(strat_algos)

        logging.info(
            ' Completed invocation of dq_new.main ..., method invocation time = {} '
                .format(epoch.get_time_delta(component_start_time)))


    def __init__(self, processing_start_time_arg):
        self.processing_start_time = processing_start_time_arg
