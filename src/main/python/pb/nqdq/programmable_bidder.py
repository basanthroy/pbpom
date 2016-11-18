__author__ = 'broy'

import requests
import fileutils
import sys
import json
import logging
import traceback
# import socks
# import socket

from config import Configuration
from config import PBArguments
from output_schema import output_schema


def create_data_map_from_output(o_sch):
    logging.info('Configuration.output_file = {}'.format(Configuration.output_file))
    file = fileutils.open_file(Configuration.output_file)

    __dataMap = {}
    __baseline_bid_map_uniques = {}
    logging.info('Constructing the datamap from the input file (output of bid mapping func)')

    for line in file:
        __line_data_map, __baseline_bid_map = o_sch.parse_output_line(line.strip())
        __mergeLineDataWithExistingData(__line_data_map, __dataMap)
        __merge_baseline_bid_data_with_uniques(__baseline_bid_map, __baseline_bid_map_uniques)

    logging.debug('__dataMap=%s',str(__dataMap)[:20])

    return __dataMap, __baseline_bid_map_uniques

def __merge_baseline_bid_data_with_uniques(__baseline_bid_map, __baseline_bid_map_uniques):

    logging.debug('__merge_baseline_bid_data_with_uniques invoked')

    for __strategy_id, __baseline_bid in __baseline_bid_map.iteritems():
        if not __baseline_bid_map_uniques.has_key(__strategy_id):
            __baseline_bid_map_uniques[__strategy_id] = __baseline_bid
    return

def __mergeLineDataWithExistingData(lineDataMap, dataMap):

    logging.debug('mergeLineDataWithExistingData invoked')

    for __strategy_id, __strategy_data in lineDataMap.iteritems():
        if __strategy_id in dataMap:
            existing_strategy_data = dataMap[__strategy_id]
            for dimension_name in __strategy_data:
                existing_strategy_data[dimension_name].update(__strategy_data[dimension_name])
        else:
            dataMap[__strategy_id] = __strategy_data
    return dataMap


def invoke_pb_for_list_data_map(configuration, org_id, dataMap, strat_bidlist, strat_algos):
    logging.info('Parsing constructed dataMap and invoking programmable bidder for each strategy/dimension pair')
    __dataMapDict = dict(dataMap)
    logging.info('__dataMapDict={}'.format(str(__dataMapDict)[:300]))
    _list_data_rest_data = []
    logging.info("invoke_pb_for_list_data_map, type = {}, dataMap={}".format(type(dataMap), dataMap))
    logging.info("invoke_pb_for_list_data_map, type = {}, strat_bidlist={}".format(type(strat_bidlist), strat_bidlist))
    logging.info("invoke_pb_for_list_data_map, type = {}, strat_algos={}".format(type(strat_algos), strat_algos))
    for __strategy_id, __strategy_data in __dataMapDict.iteritems():
        __strategy_data_dict = dict(__strategy_data)

        logging.info("str(__strategy_id) = {} strat_algos['strategy_id'] = {}".format(str(__strategy_id), strat_algos['strategy_id']))
        if str(__strategy_id) == strat_algos['strategy_id']:
            logging.info("strategy_id - {} is new. Going to invoke PB".format(__strategy_id))

            logging.info('__strategy_id = {},__strategy_data_dict={}'
                         .format(__strategy_id, str(__strategy_data_dict)[:300]))

            for __dimension_type, __dimension_data in __strategy_data_dict.iteritems():
                __request_body = {}
                # __request_body['name'] = str(__strategy_id) \
                #                          + '_' \
                #                          + str(__dimension_type) \
                #                          + '_bid_list' \
                #                          + str(time.time())

                logging.info('Constructing __price_adjustments')

                __price_adjustments = createPriceAdjustments(__strategy_id, \
                                                             __dimension_type, \
                                                             __dimension_data)

                __request_body['price_adjustments'] = __price_adjustments

                logging.info('Constructing __request_bidy_str')

                __request_body_str = json.dumps(__request_body)

                logging.info('Constructing __url')

                __url = createUrl(__dimension_type, configuration, org_id,
                                  get_bidlist_for_strategy(strat_bidlist, __strategy_id))

                # invoke_pb(configuration, __url, __request_body_str)
                _list_data_rest_data.append((__url, __request_body_str))
        else:
            logging.info("strategy_id - {} is not new. Going to skip".format(__strategy_id))

    _list_data_map = {}
    for tup in _list_data_rest_data:
        _list_data_map[tup[0]] = tup[1]

    return _list_data_map

def invoke_pb_for_baselinebid_data_map(configuration, dataMap):
    logging.info('Parsing constructed dataMap and invoking programmable bidder for each strategy/baseline_bid pair')
    _dataMapDict = dict(dataMap)
    logging.info('__dataMapDict={}'.format(str(_dataMapDict)[:300]))
    _baseline_bid_rest_data = []

    _rtb_bidding = "rtb_bidding"
    _bid_baseline = "bid_baseline"
    _base_url = configuration.api.get(Configuration.base_url_strategy)

    # Set up a proxy
    # socks.set_default_proxy(socks.SOCKS5,
    #                         configuration.api.get(Configuration.SOCKS5_PROXY_HOST),
    #                         configuration.api.get(Configuration.SOCKS5_PROXY_PORT))
    # socket.socket = socks.socksocket

    r1_headers = {"Authorization": configuration.api.get(Configuration.radiumone_api_password),
                  "Content-Type": "application/json"}

    for _strategy_id, _baseline_bid in _dataMapDict.iteritems():
        url = _base_url + "/strategies/{}"
        existing_strategy = requests.get(url.format(_strategy_id), headers=r1_headers).content
        existing_strategy = json.loads(existing_strategy)
        if existing_strategy.has_key(_rtb_bidding) and existing_strategy[_rtb_bidding].has_key(_bid_baseline):
            existing_strategy[_rtb_bidding][_bid_baseline] = float("{0:.2f}".format(float(_baseline_bid)))

        logging.info( "existing_strategy={}".format(existing_strategy))
        # r = requests.put(url.format(_strategy_id), data= json.dumps(existing_strategy), headers=r1_headers)

        _baseline_bid_rest_data.append((url.format(_strategy_id), json.dumps(existing_strategy)))

        # logging.info('r.status_code={}'.format(str(r.status_code)))
        # logging.info('r.content={}'.format(str(r.content)))

    _baseline_bid_data_map = {}
    for tup in _baseline_bid_rest_data:
        _baseline_bid_data_map[tup[0]] = tup[1]

    return _baseline_bid_rest_data

def get_bidlist_for_strategy(strat_bidlist, strategy_id):
    try:
        logging.info('Getting bid_list_id for strategy_id = {}'.format(strategy_id))
        return strat_bidlist[strategy_id]
    except:
        logging.error("get_bidlist_for_strategy threw exception = {}".format(traceback.format_exc()))

def createUrl(dimension_type, configuration, org_id, bid_list_id):
    base_url = configuration.api.get(Configuration.base_url)

    if dimension_type == 'site_domain_id2':
        return str(base_url) + '/bid_pricing/site_domains/' + str(bid_list_id)
    elif dimension_type == 'city_id':
        return str(base_url) + '/bid_pricing/cities/' + str(bid_list_id)
    elif dimension_type == 'mobile_app_id':
        return str(base_url) + '/bid_pricing/mobile_apps/' + str(bid_list_id)
    elif dimension_type == 'recency':
        return str(base_url) + '/bid_pricing/recency/' + str(bid_list_id)
    elif dimension_type == 'time_of_day':
        return str(base_url) + '/bid_pricing/time_of_day/' + str(bid_list_id)


def createPriceAdjustments(strategy_id, dimension_type, __dimension_data):

    bidding_list_name = str(strategy_id) + '_' + str(dimension_type) + '_bid_list'
    logging.info("bidding_list_name={}".format(bidding_list_name))
    __request_body_tmp = {'name' : bidding_list_name}
    __request_body_tmp['price_adj'] = 'good price'

    __price_adjustments = []

    if dimension_type == 'site_domain_id2':
        logging.debug('__dimension_data=%s',str(__dimension_data)[:20])
        for __site_domain_id, __bid_price_multiplier in __dimension_data.iteritems():
            __site_domain_obj = {'id':__site_domain_id}
            __price_adjustment = {}
            __price_adjustment['site_domain'] = __site_domain_obj
            __price_adjustment['multiplier'] = "%0.2f" % float(__bid_price_multiplier)
            __price_adjustments.append(__price_adjustment)
        return __price_adjustments
    elif dimension_type == 'city_id':
        logging.debug('__dimension_data=%s',str(__dimension_data)[:20])
        for __city_id, __bid_price_multiplier in __dimension_data.iteritems():
            __price_adjustment = {}
            __price_adjustment['city_id'] = __city_id
            __price_adjustment['multiplier'] = "%0.2f" % float(__bid_price_multiplier)
            __price_adjustments.append(__price_adjustment)
        return __price_adjustments
    elif dimension_type == 'mobile_app_id':
        logging.debug('__dimension_data=%s',str(__dimension_data)[:20])
        for __mobile_app_id, __bid_price_multiplier in __dimension_data.iteritems():
            __price_adjustment = {}
            __price_adjustment['mobile_app_id'] = __mobile_app_id
            __price_adjustment['multiplier'] = "%0.2f" % float(__bid_price_multiplier)
            __price_adjustments.append(__price_adjustment)
        return __price_adjustments
    elif dimension_type == 'recency':
        logging.debug('__dimension_data=%s',str(__dimension_data)[:20])
        for __recency, __bid_price_multiplier in __dimension_data.iteritems():
            __price_adjustment = {}
            __price_adjustment['recency_from'] = __recency.split(':')[0]
            __price_adjustment['recency_to'] = __recency.split(':')[1]
            __price_adjustment['time_unit'] = __recency.split(':')[2]
            __price_adjustment['multiplier'] = "%0.2f" % float(__bid_price_multiplier)
            __price_adjustments.append(__price_adjustment)
        return __price_adjustments
    elif dimension_type == 'time_of_day':
        logging.debug('__dimension_data= %s',str(__dimension_data)[:20])
        for __timeofday, __bid_price_multiplier in __dimension_data.iteritems():
            __price_adjustment = {}
            __price_adjustment['hour_from'] = __timeofday.split(':')[0]
            __price_adjustment['hour_to'] = __timeofday.split(':')[1]
            __price_adjustment['day_of_week'] = __timeofday.split(':')[2]
            __price_adjustment['multiplier'] = "%0.2f" % float(__bid_price_multiplier)
            __price_adjustments.append(__price_adjustment)
        return __price_adjustments


def invoke_pb(configuration, url, request_body):

    # Set up a proxy
    # socks.set_default_proxy(socks.SOCKS5,
    #                         configuration.api.get(Configuration.SOCKS5_PROXY_HOST),
    #                         configuration.api.get(Configuration.SOCKS5_PROXY_PORT))
    # socket.socket = socks.socksocket

    r1_headers = {"Authorization": configuration.api.get(Configuration.radiumone_api_password),
                  "Content-Type": "application/json"}

    logging.info('Invoking Programmable bidder endpoint. url={}, headers={}, request_body={}'.format(
                  url,
                  str(request_body[:1000]),
                  str(r1_headers)))

    r = requests.patch(url, request_body, headers=r1_headers)

    logging.info('r.status_code={}'.format(str(r.status_code)))


def main(argv, strat_bidlist, strat_algos):

    logging.info("Invoked programmable bidder main method...")

    pbArguments = PBArguments(argv)
    configuration = Configuration(pbArguments)

    o_sch = output_schema(Configuration.output_file_schema)
    logging.info('o_sch.schema={}'.format(o_sch.schema))

    __dataMap, __baseline_bid_map = create_data_map_from_output(o_sch)

    _list_data_rest_data = invoke_pb_for_list_data_map(configuration,
                                                        pbArguments.get_org_id(),
                                                        __dataMap, strat_bidlist, strat_algos)
    _baseline_bid_rest_data = invoke_pb_for_baselinebid_data_map(configuration, __baseline_bid_map)
    return _list_data_rest_data, _baseline_bid_rest_data


if __name__ == '__main__':
    logging.info('Invoking programmable bidder R1 API endpoints')
    logging.info('Arg list={}'.format(str(sys.argv)))
    # main(sys.argv)
    pbargs = PBArguments(["org:us", "env:prod"])
    print pbargs
    configuration = Configuration(pbargs)
    o_sch = output_schema(Configuration.output_file_schema)
    logging.info('o_sch.schema={}'.format(o_sch.schema))

    _dataMap, _baseline_bid_map = create_data_map_from_output(o_sch)
    invoke_pb_for_baselinebid_data_map(configuration, _baseline_bid_map)

