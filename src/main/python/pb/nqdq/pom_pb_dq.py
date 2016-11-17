__author__ = 'broy'

import logging
import sys
import time
import traceback

from pb.config import pb_properties

import epoch
from dq_helper import dq_helper_class
from dq_new import dq_new_class


def main(argv):

    processing_start_time = int(time.time())
    logging.info('Starting dequeue, time={}, formatted time = {}'.format(str(processing_start_time), (
    time.strftime("%Y %m %e %H %M %S", time.localtime(processing_start_time)))))

    dqhelper = dq_helper_class(processing_start_time)
    dqnew = dq_new_class(processing_start_time)

    strat_algos = {}
    strat_bidlist = dict()

    try:

        strat_algos = dqhelper.poll_queue()
        logging.info(" strategy records in DOM_REC_QUEUE polled. Values = {}".format(strat_algos))

        if len(strat_algos) == 0:
            logging.info(" No new strategy records found in DOM_REC_QUEUE. Exiting this run...")
            return

        strat_bidlist = dqhelper.copy_opti_strats_to_hive(strat_algos)

        dqnew.main(strat_algos)

        dqhelper.join_strats_to_preds()

        dqhelper.invoke_mapping_func()

        _list_data_rest_data, _baseline_bid_rest_data = \
            dqhelper.invoke_pb_endpoint(strat_bidlist, strat_algos)

        # dqhelper.log_output_to_hive()
        #
        # dqhelper.update_strategies_to_modeled(strat_bidlist, strat_algos)

        _dom_rec_result_id = dqhelper.persist_result_data(strat_algos,
                                                          _list_data_rest_data,
                                                          _baseline_bid_rest_data)

        dqhelper.update_request_status(strat_bidlist, strat_algos, 'processed',
                                       _dom_rec_result_id)

        logging.info(' Finished pipeline tasks, total elapsed time = {} '.format(
            epoch.get_time_delta(processing_start_time)))

        # rest_data_map = {}
        # rest_data_map["list_data_rest_data"] = _list_data_rest_data
        # rest_data_map["baseline_bid_rest_data"] = _baseline_bid_rest_data
        # return rest_data_map

    except:
        logging.error("Exception in main method of pb_dq.")
        logging.error("Setting the PB_QUEUE rows to error status.")
        logging.error(traceback.format_exc())

        dqhelper.update_request_status(strat_bidlist, strat_algos, 'error')


if __name__ == '__main__':
    logging.basicConfig(filename=pb_properties.ROOT_DIR + '/log/dq.log',
                        level=logging.INFO,
                        format='%(asctime)s %(name)s (%(levelname)s): %(message)s')
    logging.info('\n\n\n')
    logging.info(' Beginning dequeue main method ')
    main(sys.argv)
    logging.info('### Completed dequeue main method ### ')


