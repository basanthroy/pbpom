__author__ = 'cliu'

import time


def get_current_time_up_to_date(epoch_time):
    return time.strftime('%Y%m%d', time.localtime(epoch_time))


def get_current_time_up_to_second(epoch_time):
    return time.strftime('%Y%m%d%H%M%S', time.localtime(epoch_time))


def get_current_time_prefix(epoch_time):
    return get_current_time_up_to_hundredth_of_a_second(epoch_time) + '_'


def get_current_time_up_to_hundredth_of_a_second(epoch_time):
    current_time_up_to_second = get_current_time_up_to_second(epoch_time)

    epoch_time_str = str(epoch_time)
    if '.' not in epoch_time_str:
        epoch_time_str += '.'
    epoch_time_str += '00'  # add two zeros

    return current_time_up_to_second + \
           epoch_time_str.split(".")[1][0:2]  # truncate to hundredth of a second


def get_time_delta(reference_time):
    return int(time.time()) - reference_time