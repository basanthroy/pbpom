__author__ = 'broy'

import MySQLdb
import logging
import traceback
import time

from pb.config import pb_properties

def main():

    logging.info("PB Enqueue Main method")

    adpred_db_connect = MySQLdb.connect(pb_properties.adpred_db_connect_host,
                                pb_properties.adpred_db_connect_user,
                                pb_properties.adpred_db_connect_password,
                                pb_properties.adpred_db_connect_db)
    adpred_db_connect.autocommit(False)
    cursor_adpred = adpred_db_connect.cursor()

    opti_db_connect = MySQLdb.connect(pb_properties.opti_db_connect_host,
                                    pb_properties.opti_db_connect_user,
                                    pb_properties.opti_db_connect_password,
                                    pb_properties.opti_db_connect_db)

    opti_db_connect.autocommit(False)
    cursor_opti = opti_db_connect.cursor()

    try:

        find_and_insert_new_strategies_to_queue(cursor_opti, cursor_adpred)

        adpred_db_connect.commit()
        opti_db_connect.commit()

    except:
        logging.error('')
        logging.error(' Exception occurred. Rollback ..')
        adpred_db_connect.rollback()
        logging.error(traceback.format_exc())

    cursor_adpred.close()
    cursor_opti.close()
    logging.info('')


def find_and_insert_new_strategies_to_queue(cursor_opti, cursor_adpred):

    logging.info("find_and_insert_new_strategies_to_queue..")

    try:

        select_modified_strategies_statement = """
                      SELECT
                        strategy_id,
                        COALESCE(algorithm_id, -1) as algo_id,
                        'domain' as dimension_type,
                        'new' as event_type,
                        DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as create_ts,
                        DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as update_ts,
                        'pending' as status
                      FROM programmable_bid_domainbidderinputfield where modeled = 0
        """

        recency_union = """
                              UNION
                      SELECT
                        strategy_id,
                        algorithm_id as algo_id,
                        'recency' as dimension_type,
                        'new' as event_type,
                        DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as create_ts,
                        DATE_FORMAT(NOW(),'%Y%m%d%H%i%s00') as update_ts,
                        'pending' as status
                      FROM programmable_bid_recencybidderinputfield where modeled = 0
        """

        num_strategies_selected = cursor_opti.execute(select_modified_strategies_statement)

        logging.info("find_and_insert_new_algos_to_queue, num_strategies_selected={}".format(num_strategies_selected))

        rows = []
        for row in cursor_opti:
            rows.append(row)
            logging.info(" New strategy found : {}".format(str(row)))

        insert_modified_strategies_statement = """
                      insert into PB_QUEUE(strategy_id, algo_id, dimension_type, event_type, create_ts, update_ts, status)
                      values (%s, %s, %s, %s, %s, %s, %s)
        """

        num_strategies_inserted = cursor_adpred.executemany(insert_modified_strategies_statement, rows)

        logging.info('find_and_insert_new_algos, num_strategies_inserted={}'.format(num_strategies_inserted))
    except:
        logging.error("find_and_insert_new_strategies_to_queue exception, exception={}".format(traceback.format_exc()))



if __name__ == '__main__':
    logging.basicConfig(filename=pb_properties.ROOT_DIR + '/log/nq.log',
                        level=logging.INFO,
                        format='%(asctime)s %(name)s (%(levelname)s): %(message)s')
    logging.info('\n\n\n')
    logging.info('Module Enqueue invoked')

    processing_start_time = int(time.time())
    logging.info('Starting dequeue, time={}, formatted time = {}'.format(str(processing_start_time), (time.strftime("%Y %m %e %H %M %S", time.localtime(processing_start_time)))))

    main();