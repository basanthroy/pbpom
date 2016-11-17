__author__ = 'nromano' # Chu-Chi: modified nicole's code to fit my case :D

import sys,os
from subprocess import PIPE, Popen
#import r1_framework as f
from r1_framework.util_date import *

##########################################
# For use when you need to drop many un-needed partitions from 1+ tables in a db
# Two ways to use:
#
# 1. Just display the code (so you can make sure it's what you want), and then copy/paste into hive
#    /opt/python2.7/bin/python
#    >> import drop_partitions
#    >> statement = drop_partitions.create_statement(2, 'm', 'chuchi_tmp', ['output_log'])
#    >> print statement
#
# 2. Programmatically drop partitions
#    all of the above, then:
#    >> execute_statement(statement)
#
# note: only works for algo, dt partitions
##########################################

is_debug_mode = False  # True

def create_statement(n, units, db, table_list):

    ##############################
    # n: # of days or months of partitions to keep
    # units: 'd' for days, 'm' for months
    # db: database name
    # table_list: list of tables to drop from
    ##############################

    units = convert_to_days(units)
    to_part = date_to_dt(add_delta_date(current_datetime(), (-int(n) * int(units) * 60 * 60))) # identify the first partition that should be kept

    if type(table_list) is str:
        table_list = [table_list]

    for table in table_list:

        parts_list = get_partitions(db, table) # get list of all partitions

        to_drop = parts_to_drop(parts_list, to_part) # truncate list to only those that should be dropped

        if is_debug_mode:
            print 'create_statement\'s table in table_list:'+str(format(table     ))
            # e.g.,                                                     output_log
            ############################################################################################################

        if is_debug_mode:
            print 'create_statement\'s ... about to_drop'
            for date in to_drop:
                print 'create_statement\'s date to_drop:'+str(date                   )
                # e.g.,                                       algo_id=901/dt=20160509
                # p.s., those being displayed are n units old (e.g., n can be 1 and units can be month)
                ########################################################################################################

        drop_statements = ['   alter table {0}        drop if exists partition('.format(table) +
                           str(date                   ).replace('/', ',') + ')' for date in to_drop] # generate hive cmd
        # e.g.,                alter table output_log drop if exists partition(
        #                      algo_id=901,dt=20160509                      );
        ################################################################################################################

        statement = 'use {0};\n'.format(db) + ';\n'.join(drop_statements) + ';'  # join all the hive cmd into one line

        if is_debug_mode:
            print 'beging *********************************************************************************************'
            print 'create_statement\'s statement:'+statement  # This is the hive sql cmd to use to drop the partitions !
            print 'end ************************************************************************************************'

        return statement, to_drop

def execute_statement(statement):

    proc = Popen(['hive', '-e', statement], stdout=PIPE)

    output = proc.communicate()


##################################################
############### helper functions #################
##################################################

def convert_to_days(units):
    if units == 'm':
	return 31 * 24
    elif units == 'd':
	return 24
    elif units == 'h':
	print "hours are not supported"
    else:
	print "units must be 'm' or 'd'"

def get_partitions(db, table):

    ######################################
    # pulls list of partitions from hive #
    # checks that they are dt partitions #
    ######################################

    statement = 'use {0}; show partitions {1}'.format(db, table)

    proc = Popen(['hive', '-e', statement], stdout=PIPE)
    output = proc.communicate()

    if is_debug_mode:
        print 'begin **************************************************************************************************'
        print 'get_partitions\'s output[0]: ' + str(output[0])
        print 'end   **************************************************************************************************'

    if is_debug_mode:
        for part in output[0].split('\n'):
            if 'algo_id' in part and 'dt=' in part:
                print 'get_partition\'s output[0]\'s split: ' + part
                # e.g.,              algo_id=901/dt=20160609
                # p.s., every partition, not just those going to purge !!!
                ########################################################################################################

    partitions_list = [str(part.strip()) for part in output[0].split('\n') if 'dt=' in part and 'algo_id=' in part]

    if is_debug_mode:
        for partition in partitions_list:
            print 'get_partition\'s partitions_list: ' + partition
            # e.g.,              algo_id=901/dt=20160609
            # p.s., every partitions, not those going to purge !!!
            ############################################################################################################

    return partitions_list


def parts_to_drop(partitions_list, to_part):

    ####################################################################################################################
    # takes a list of partitions (e.g. [20150914, 20150915, 20150916]) and the first date that should be kept (20150916)
    # returns a list of only partitions to drop
    ####################################################################################################################

    if is_debug_mode:
        for part in partitions_list:
	        print 'parts_to_drop\'s partitions_list contains: ' + part.split('dt=')[1].strip() + \
                  ' (will be removed if it\' less than to_part:' + to_part + ')'
            # e.g.,                        20160609
            # p.s., every partition, not just those going to purge !!!
            ############################################################################################################

    # if is_debug_mode:
    #     for part in partitions_list:
    #         print '1:'
    #         print int(to_part)
    #         print int(part.split('dt=')[1].strip())
    #         print int(part.split('dt=')[1].strip()) > int(to_part)
    #         if int(part.split('dt=')[1].strip()) < int(to_part):
    #             print '2:'+part

    return [part for part in partitions_list if int(part.split('dt=')[1].strip()) < int(to_part)]
