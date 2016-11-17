__author__ = 'broy'

import logging
import os
import time
import traceback
import json

import MySQLdb
import pyhs2
from pb.config import pb_properties

import arte_bid_price_mapping
import epoch
import programmable_bidder
from util import Util


class dq_helper_class:

    processing_start_time = 0

    def poll_queue(self):

        component_start_time = int(time.time())

        adpred_db_connect = MySQLdb.connect(pb_properties.adpred_db_connect_host,
                                            pb_properties.adpred_db_connect_user,
                                            pb_properties.adpred_db_connect_password,
                                            pb_properties.adpred_db_connect_db)

        adpred_db_connect.autocommit(False)
        cursor = adpred_db_connect.cursor()

        # strat_algo = {}

        util = Util()

        try:
            # reservation_details_statement = """
            #                                      SELECT DISTINCT
            #                                      strategy_id,algo_id, dimension_type, event_type
            #                                      FROM PB_QUEUE
            #                                      WHERE status = "pending"
            #                                      """

            _process_name = util.generate_process_name()

            logging.info("In poll_queue, _process_name = {}".format(_process_name))

            reserve_row_statement = """
                UPDATE
                    DOM_REC_QUEUE
                    JOIN (SELECT MIN(dom_rec_id) as dom_rec_id
                          FROM DOM_REC_QUEUE
                          WHERE status = 'new'
                          AND NOT EXISTS
                              (SELECT 1 from DOM_REC_QUEUE where status = 'in_process')
                         ) as min_dom_rec
                    USING(dom_rec_id)
                SET process_name = '{}',
                    status       = 'in_process',
                    date_in_process = now()
            """.format(_process_name)

            cursor.execute(reserve_row_statement)

            _columns = "dom_rec_id, bid_list_name, bid_list_id, strategy_name, line_id," + \
                       "target_cpm, uniques, min_bid, max_bid, strategy_id, status, " \
                       "process_name, date_new, date_in_process, date_processed, date_error"

            reservation_details_statement = """
                SELECT
                    {}
                FROM DOM_REC_QUEUE
                WHERE process_name = '{}'
            """.format(_columns, _process_name)

            logging.info(
                ' In db, adpred_db_connect, executing the sql statement: ' + reservation_details_statement)
            cursor.execute(reservation_details_statement)

            dom_rec_queue_row = {}

            for row in cursor:
                # strategy_id_str = str(row[0])
                # algo_id_str = str(row[1])
                # dimension_type_str = str(row[2])
                # event_type_str = str(row[3])
                # logging.info('')
                # logging.info(' The earliest time that Algorithm ' + algo_id_str +
                #              ' \twas requested and still pending is at ' + dimension_type_str)
                # strat_algo[strategy_id_str] = algo_id_str

                _column_names = [_column_name.strip() for _column_name in  _columns.split(",")]

                dom_rec_queue_row = {k:v if v != None else "-1" for k,v in zip(_column_names, row)}
                # dom_rec_queue_row.update((x, y * 2) for x, y in dom_rec_queue_row.items())

                logging.info("type={}".format(type(dom_rec_queue_row)))

                logging.info('row reservation was successful. Queue row = {}'.format(row))
                # self.update_join_request_status_given_cursor(cursor, strategy_id_str, algo_id_str,
                #                                         dimension_type_str, event_type_str)

            logging.info('')
            logging.info(' Going to commit (i.e., release the lock)')
            adpred_db_connect.commit()  # Note: commit (i.e., release the lock) right after select for update
        except:
            logging.info('')
            logging.info(' Going to rollback (i.e., release the lock)')
            adpred_db_connect.rollback()  # Note: rollback (i.e., release the lock) if anything goes wrong
            logging.error(traceback.format_exc())
            raise

        cursor.close()

        logging.info(' Afer poll_queue, total elapsed time = {}, poll_queue method execution time = {}, dom_rec_queue_row = {}'
                     .format(epoch.get_time_delta(self.processing_start_time), epoch.get_time_delta(component_start_time), dom_rec_queue_row))

        return dom_rec_queue_row


    def copy_opti_strats_to_hive(self, dom_rec_queue_row):

        logging.info(' In copy_opti_strats_to_hive method ')

        component_start_time = int(time.time())

        # opti_db_connect = MySQLdb.connect(pb_properties.opti_db_connect_host,
        #                                   pb_properties.opti_db_connect_user,
        #                                   pb_properties.opti_db_connect_password,
        #                                   pb_properties.opti_db_connect_db)
        #
        # opti_db_connect.autocommit(False)
        # cursor = opti_db_connect.cursor()
        #
        # select_stmt = """
        #       select domain.strategy_id,
        #              domain.bid_list_id,
        #              domain.line_id,
        #              COALESCE(uniques.number_of_unique_users, -1) as uniques_count,
        #              login.email,
        #              domain.min_bid,
        #              domain.max_bid,
        #              COALESCE(domain.algorithm_id, -1),
        #              1 as dimension_id,
        #              domain.target_cpm
        #              from programmable_bid_domainbidderinputfield domain
        #              LEFT JOIN programmable_bid_uniquesselection uniques
        #              ON domain.uniques_id = uniques.id
        #              JOIN login_optimizer login
        #              ON domain.user_id = login.id
        #              """
        #
        # logging.info(' In copy_opti_strats_to_hive method, before cursor execute ')
        #
        # cursor.execute(select_stmt)

        stack_values = ""
        # num_rows_processed_from_mysql = 0

        strat_bidlist = dict()

        # for row in cursor:
        #     logging.info('opti table row={}'.format(row))
        #     if (num_rows_processed_from_mysql > 0):
        #         stack_values += " , "
        #     stack_values += " {}, {}, {}, {}, '{}', {}, {}, {}, {}, double(0{}) ".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
        #     num_rows_processed_from_mysql += 1
        #
        #     strat_bidlist[str(row[0])] = str(row[1])
        #
        #     logging.debug('columns={}, {}, {}, {}, {}, {}, {}, {}, {}, {}'.format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))

        logging.info('opti table row={}'.format(dom_rec_queue_row))
        # if (num_rows_processed_from_mysql > 0):
        #     stack_values += " , "
        stack_values += " {}, {}, {}, {}, '{}', {}, {}, {}, {}, double(0{}) "\
            .format(dom_rec_queue_row["strategy_id"],
                    dom_rec_queue_row["bid_list_id"],
                    dom_rec_queue_row["line_id"],
                    dom_rec_queue_row["uniques"],
                    "broy@radiumone.com",
                    dom_rec_queue_row["min_bid"],
                    dom_rec_queue_row["max_bid"],
                    -1,
                    1,
                    dom_rec_queue_row["target_cpm"])
        num_rows_processed_from_mysql = 1

        strat_bidlist[str(dom_rec_queue_row["strategy_id"])] = str(dom_rec_queue_row["bid_list_id"])

        hive_insert_statement = """
            INSERT OVERWRITE TABLE
            {}.pom_programmable_bid_domainbidderinputfield
            SELECT stack({} , {})
            AS(`strategy_id`, `bid_list_id`, `line_id`, `unique_count`, `email`, `min_bid`, `max_bid`, `algorithm_id`, `dimension_id`, `target_ecpm`)
            FROM radiumone_master.strategies limit {}
            """.format(pb_properties.hive_db,
                       num_rows_processed_from_mysql, stack_values, num_rows_processed_from_mysql)

        logging.info("hive_insert_statement={}".format(hive_insert_statement))

        with pyhs2.connect(host=pb_properties.hive_server_host,
                           user=pb_properties.hive_server_user,
                           password=pb_properties.hive_server_password,
                           port=pb_properties.hive_server_port,
                           authMechanism=pb_properties.hive_server_auth) as conn:
            with conn.cursor() as cur:

                logging.info('')
                logging.info(
                    ' In host, opti_db, executing the hql statement: ' +
                    hive_insert_statement)

                cur.execute(hive_insert_statement)

        logging.info("copy_opti_strats_to_hive, strat_bidlist={}".format(strat_bidlist))

        logging.info(
            ' copy_opti_strats_to_hive completed.., elapsed time = {} , copy_opti_strats_to_hive method execution time = {}'
                .format(epoch.get_time_delta(self.processing_start_time), epoch.get_time_delta(component_start_time)))

        return strat_bidlist


    def join_strats_to_preds(self):

        logging.info(' In join_strats_to_preds. Beginning ')

        component_start_time = int(time.time())
        dt = epoch.get_current_time_up_to_date(self.processing_start_time)
        current_timestamp_up_to_hundredth_of_a_second = epoch.get_current_time_up_to_hundredth_of_a_second(
            self.processing_start_time)

        select_stmt = """
        select preds.strategy_id,
        algo_id,
        ARRAY(NAMED_STRUCT("dimension_type_id", dimension_value_structs.dimension_type_id, "dimension_value", dimension_value_structs.dimension_value.first)),
        cast(probability_score as string),
        cast(min_bid as string),
        cast(max_bid as string),
        cast(strats.strategy_id as string),
        cast(bid_list_id as string),
        cast(algorithm_id as string),
        cast(dimension_id as string),
        cast(COALESCE (strats.target_ecpm, 1) as string)
        from {}.pom_pb_dimension_predictions preds
        join {}.pom_programmable_bid_domainbidderinputfield strats
        on (preds.strategy_id = strats.strategy_id)
        """.format(pb_properties.hive_db, pb_properties.hive_db)

        logging.info(' In join_strats_to_preds. After stmt def ')

        with pyhs2.connect(host=pb_properties.hive_server_host,
                       user=pb_properties.hive_server_user,
                       password=pb_properties.hive_server_password,
                       port=pb_properties.hive_server_port,
                       authMechanism=pb_properties.hive_server_auth) as conn:

            logging.info(' In join_strats_to_preds. After connect ')

            with conn.cursor() as cur:

                logging.info(' In join_strats_to_preds. After cur ')

                logging.info('')
                logging.info(
                    ' In host, jobserver, executing the hql statement: ' +
                    select_stmt)

                results = cur.execute(select_stmt)

                logging.info("results={}".format(str(results)[:30]))

                # file = open(pb_properties.ROOT_DIR + '/log/input.fsv', 'w')
                file2 = open(pb_properties.ROOT_DIR + '/log/input.fsv', 'w')

                logging.info("executed")

                for i in cur.fetch():
                    logging.debug("i=%s", str(i)[:2])
                    # file.write("{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}"
                    #            .format(i[0]
                    #                ,'\001',i[1]
                    #                ,'\001',i[2]
                    #                ,'\001',i[3]
                    #                ,'\001',i[4]
                    #                ,'\001',i[5]
                    #                ,'\001',dt
                    #                ,'\001',current_timestamp_up_to_hundredth_of_a_second) + '\n')
                    file2.write("{}{}{}{}{}{}{}{}{}{}{}{}{}"
                               .format(i[0]
                                       , '\001', i[1]
                                       , '\001', i[2]
                                       , '\001', i[3]
                                       , '\001', i[10]
                                       , '\001', dt
                                       , '\001', current_timestamp_up_to_hundredth_of_a_second) + '\n')

                # file.close()
                file2.close()

        logging.info(' Completed invocation of join_strats_to_preds ..., total elapsed time = {} , method execution time = {}'.format(
            epoch.get_time_delta(self.processing_start_time), epoch.get_time_delta(component_start_time)))



    def invoke_mapping_func(self):
        logging.info('About to invoke arte_bid_price_mapping function')
        component_start_time = int(time.time())

        arte_bid_price_mapping.main(pb_properties.ROOT_DIR + '/log/input.fsv', pb_properties.ROOT_DIR + '/log/output.fsv', pb_properties.ROOT_DIR + '/log/log.json')

        logging.info(' Completed invocation of invoke_mapping_func..., cumulative elapsed time = {}, method invocation time = {} '
                     .format(epoch.get_time_delta(self.processing_start_time), epoch.get_time_delta(component_start_time)))



    def invoke_pb_endpoint(self, strat_bidlist, strat_algos):
        logging.info('About to invoke programmable bidder endpoint')

        component_start_time = int(time.time())

        argv = []
        argv.append("env:prod")
        _list_data_rest_data, _baseline_bid_rest_data = programmable_bidder.main(argv, strat_bidlist, strat_algos)

        logging.info(' Completed invocation of .. dq_new.main, cumulative elapsed time = {} , method execution time = {}'
                     .format(epoch.get_time_delta(self.processing_start_time),
                             epoch.get_time_delta(component_start_time)))

        return _list_data_rest_data, _baseline_bid_rest_data



    def log_output_to_hive(self):
        logging.info(' About to invoke .. log_output_to_hive')
        component_start_time = int(time.time())

        dt = epoch.get_current_time_up_to_date(self.processing_start_time)
        current_timestamp_up_to_hundredth_of_a_second = epoch.get_current_time_up_to_hundredth_of_a_second(
            self.processing_start_time)

        os.system("hdfs dfs -mkdir -p /user/arteu/programmablebidder/prod/log/algo_id=1/dt={}".format(dt))
        os.system("hdfs dfs -mkdir -p /user/arteu/programmablebidder/prod/input/algo_id=1/dt={}".format(dt))
        os.system("hdfs dfs -mkdir -p /user/arteu/programmablebidder/prod/output/algo_id=1/dt={}".format(dt))
        os.system("mv {}/log/log.json {}/log/log.{}.json".format(pb_properties.ROOT_DIR, pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second))
        os.system("mv {}/log/input.fsv {}/log/input.{}.fsv".format(pb_properties.ROOT_DIR, pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second))
        os.system("mv {}/log/output.fsv {}/log/output.{}.fsv".format(pb_properties.ROOT_DIR, pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second))
        os.system("hdfs dfs -copyFromLocal {}/log/log.{}.json /user/arteu/programmablebidder/prod/log/algo_id=1/dt={}/".format(pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second, dt))
        os.system("hdfs dfs -copyFromLocal {}/log/input.{}.fsv /user/arteu/programmablebidder/prod/input/algo_id=1/dt={}/".format(pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second, dt))
        os.system("hdfs dfs -copyFromLocal {}/log/output.{}.fsv /user/arteu/programmablebidder/prod/output/algo_id=1/dt={}/".format(pb_properties.ROOT_DIR, current_timestamp_up_to_hundredth_of_a_second, dt))
        os.system('hive -S -e "use ' + ' {} '.format(pb_properties.hive_db) + '; msck repair table ' + ' pb_output_log ' + '"')

        logging.info(' Completed invocation of .. log_output_to_hive, cumulative elapsed time = {}, method invocation time = {} '
                     .format(epoch.get_time_delta(self.processing_start_time),
                             epoch.get_time_delta(component_start_time)))



    def update_strategies_to_modeled(self, strat_bidlist, strat_algos):

        logging.info(' In update_strategies_to_modeled method ')
        component_start_time = int(time.time())

        opti_db_connect = MySQLdb.connect(pb_properties.opti_db_connect_host,
                                          pb_properties.opti_db_connect_user,
                                          pb_properties.opti_db_connect_password,
                                          pb_properties.opti_db_connect_db)

        opti_db_connect.autocommit(False)
        cursor = opti_db_connect.cursor()

        update_stmt_template = """
            update programmable_bid_domainbidderinputfield
            set modeled = 1
            where strategy_id = {}
            and bid_list_id = {}
            """

        try:

            strat_bidlist_filtered = {strategy: strat_bidlist[strategy]
                                      for strategy in strat_bidlist.keys() if
                                      str(strategy) in strat_algos.keys()}

            for strategy_id, bid_list_id in strat_bidlist_filtered.iteritems():
                update_stmt = update_stmt_template
                update_stmt = update_stmt.format(strategy_id, bid_list_id)

                logging.info(' In update_strategies_to_modeled method, before cursor execute, strategy_id, bidlist_id = {} , {} '.format(strategy_id, bid_list_id))

                num_rows_updated = cursor.execute(update_stmt)

                logging.info("update_stmt = {}".format(update_stmt))

                logging.info("""Set modeled = 1 for strategy_id = {} and bid_list_id = {}.
                                Number of rows = {}""".format(strategy_id, bid_list_id, num_rows_updated))

        except:
            logging.error('Exception when setting modeled = 1; exception = {}'.format(traceback.format_exc()))
            raise

        opti_db_connect.commit()

        logging.info(' Completed invocation of .. update_strategies_to_modeled, cumulative elapsed time = {}, method invocation time = {} '
            .format(
            epoch.get_time_delta(self.processing_start_time),
            epoch.get_time_delta(component_start_time)))


    def update_request_status(self, strat_bidlist, strat_algos, request_status,
                              _dom_rec_result_id=-1):

        logging.info(' In update_request_status method, with request_status = {} '
                     .format(request_status))
        logging.info("strat_bidlist={}".format(strat_bidlist))
        logging.info("strat_algos={}".format(strat_algos))

        component_start_time = int(time.time())

        adpred_db_connect = MySQLdb.connect(pb_properties.adpred_db_connect_host,
                                          pb_properties.adpred_db_connect_user,
                                          pb_properties.adpred_db_connect_password,
                                          pb_properties.adpred_db_connect_db)

        adpred_db_connect.autocommit(False)
        cursor = adpred_db_connect.cursor()

        if _dom_rec_result_id == -1:
            _dom_rec_result_id_str = 'NULL'
        else:
            _dom_rec_result_id_str = _dom_rec_result_id

        logging.info("request_status = {}, strat_algos = {}, _dom_rec_result_id_str = {}".format(request_status, strat_algos, _dom_rec_result_id_str))

        update_stmt = """
            update DOM_REC_QUEUE
            set status = '{}',
            dom_rec_result_id = {}
            where dom_rec_id = {}
            """.format(request_status, strat_algos['dom_rec_id'], _dom_rec_result_id_str)

        try:

            # strat_bidlist_filtered = {strategy: strat_bidlist[strategy]
            #                           for strategy in strat_bidlist.keys() if
            #                           str(strategy) in strat_algos.keys()}
            #
            # logging.info("strat_bidlist_filtered={}"
            #              .format(strat_bidlist_filtered))
            #
            # for strategy_id, bid_list_id in strat_bidlist_filtered.iteritems():
            #     update_stmt = update_stmt_template
            #     update_stmt = update_stmt.format(request_status, strategy_id)
            #
            #     logging.info(' In update_request_status method, before cursor execute, strategy_id, bidlist_id = {} , {} '.format(strategy_id, bid_list_id))
            #
            #     num_rows_updated = cursor.execute(update_stmt)
            #
            #     logging.info("update_stmt = {}".format(update_stmt))
            #
            #     logging.info("""Set modeled = 1 for strategy_id = {} and bid_list_id = {}.
            #                     Number of rows = {}""".format(strategy_id, bid_list_id, num_rows_updated))

            _num_rows_updated = cursor.execute(update_stmt)

            logging.info("_num_rows_updated = {}, update_stmt = {}"
                         .format(_num_rows_updated, update_stmt))

        except:
            logging.error('Exception when setting modeled = 1; exception = {}'.format(traceback.format_exc()))
            raise

        adpred_db_connect.commit()

        logging.info(' Completed invocation of .. update_request_status, cumulative elapsed time = {}, Method invocation time = {} '
            .format(
            epoch.get_time_delta(self.processing_start_time),
            epoch.get_time_delta(component_start_time)))


    def persist_result_data(self, _strat_algos, _list_data_rest_data, _baseline_bid_rest_data):

        logging.info(' In persist_result_data method ')
        logging.info("_strat_algos={}".format(_strat_algos))
        logging.info("_list_data_rest_data={}".format(_list_data_rest_data))
        logging.info("_baseline_bid_rest_data={}".format(_baseline_bid_rest_data))

        component_start_time = int(time.time())

        adpred_db_connect = MySQLdb.connect(pb_properties.adpred_db_connect_host,
                                          pb_properties.adpred_db_connect_user,
                                          pb_properties.adpred_db_connect_password,
                                          pb_properties.adpred_db_connect_db)

        adpred_db_connect.autocommit(False)
        cursor = adpred_db_connect.cursor()

        try:

            logging.info(' In update_request_status method, before cursor execute ')

            _insert_stmt = """
                insert into
                DOM_REC_RESULTS (dom_rec_id, list_data, baseline_bid_data)
                values ({}, '{}', '{}')
                """.format(_strat_algos['dom_rec_id'],
                           json.dumps(_list_data_rest_data),
                           json.dumps(_baseline_bid_rest_data))

            logging.info("_insert_stmt = {}".format(_insert_stmt))

            cursor.execute(_insert_stmt)

            num_rows_updated = cursor.execute(_insert_stmt)

            logging.info("num_rows_updated = {}".format(num_rows_updated))

            _select_statement = """
                select dom_rec_result_id from
                DOM_REC_RESULTS
                WHERE dom_rec_id = {}
                """.format(_strat_algos['dom_rec_id'])

            cursor.execute(_select_statement)

            for row in cursor:
                _dom_rec_result_id = row[0]

        except:
            logging.error('Exception when persisting domain recommendation results; exception = {}'
                          .format(traceback.format_exc()))
            raise

        adpred_db_connect.commit()

        logging.info(' Completed invocation of .. update_request_status, cumulative elapsed time = {}, Method invocation time = {} '
            .format(
            epoch.get_time_delta(self.processing_start_time),
            epoch.get_time_delta(component_start_time)))

        return _dom_rec_result_id

    def __init__(self, processing_start_time_arg):
        logging.info("dq_helper instantiated...")
        self.processing_start_time = processing_start_time_arg