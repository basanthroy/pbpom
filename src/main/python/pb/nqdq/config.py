__author__ = 'broy'

import logging

from pb.config import pb_properties

site_domain_id2='site_domain_id2'

dimension_names = [site_domain_id2]

class PBArguments:
    env = 'env'
    org = 'org'
    __argMap = {env : ''}

    def get(self, key):
        return self.__argMap.get(key)

    def isProdEnv(self):
        return self.__argMap.get(self.env) == 'prod'

    def isStagingEnv(self):
        return self.__argMap.get(self.env) == 'stg'

    def get_org_id(self):
        return self.__argMap.get(self.org)

    def __init__(self, argv):
        self.__argMap[self.org] = '1'
        for arg in argv:
            logging.info("PBArguments, arg = {}".format(arg))
            if (len(arg.split(':')) > 1):
                argName = arg.split(':')[0]
                argValue = arg.split(':')[1]
                logging.info('argName={}, argValue={}'.format(argName, argValue))
                self.__argMap[argName] = argValue


class Configuration:
    # output_file = '/Users/broy/temp/output.fsv'
    # output_file_schema = '/Users/broy/PycharmProjects/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/pb/nqdq/output_type.tsv'

    # TODO : UNCOMMENT THIS BEFORE CHECKIN
    output_file = pb_properties.ROOT_DIR + '/log/output.fsv'
    output_file_schema = pb_properties.SCRIPT_DIR + '/src/main/python/pb/nqdq/output_type.tsv'

    SOCKS5_PROXY_HOST = 'SOCKS5_PROXY_HOST'
    SOCKS5_PROXY_PORT = 'SOCKS5_PROXY_PORT'
    radiumone_api_password = 'radiumone_api_password'
    base_url = 'base_url'
    base_url_strategy = 'base_url_strategy'

    __api_stg = {
        # Configuration
        SOCKS5_PROXY_HOST: 'localhost',
        SOCKS5_PROXY_PORT: 8080,
        radiumone_api_password: pb_properties.radiumone_api_password_stg,
        base_url: pb_properties.base_url_stg,
        base_url_strategy: pb_properties.base_url_stg_strategy,
    }

    __api_prod = {
        # Configuration
        SOCKS5_PROXY_HOST: 'localhost', #invalid_host',
        SOCKS5_PROXY_PORT: 8080,
        radiumone_api_password: pb_properties.radiumone_api_password_prod, #'INVALID_PASSWORD'
        base_url: pb_properties.base_url_prod,
        base_url_strategy: pb_properties.base_url_prod_strategy
    }

    api = {}

    def __init__(self, pbArguments):
        if pbArguments.isProdEnv():
            logging.info('Production Environment')
            self.api = self.__api_prod
        elif pbArguments.isStagingEnv():
            logging.info('Staging Environment')
            self.api = self.__api_stg

