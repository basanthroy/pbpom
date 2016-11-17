__author__ = 'broy'

import logging
import re
from collections import OrderedDict

import fileutils


# Need the schema file to be in a specific format.
# 1. It needs to contain a series of name-value pairs.
# 2. The name-value pairs can be in any order
# 3. Exactly one of the values needs to be 'Array'
# 4. The 'Array' value contains the strategy_id:bid_price_multiplier
# 5. All the non - 'Array' - values need to be dimension names / dimension datatype
# 6. For Recency and Time-Of-Day dimension types. the dimension id are of formats
#    2:55:days  -> Implying recency_from: 2, recency_to: 55, time_unit: days
#    2:7:monday -> Implying hour_from: 2, hour_to: 7, day_of_week: monday

class output_schema:

    schema = OrderedDict()
    dimension_id_indexes = []
    strategyIdMultiplierIndex = -1

    def construct_schema(self, str):
        logging.info('schema str={}'.format(str))
        parts = str.split('\t')

        index = 0

        for part in parts:
            part = part.strip()
            part_name = re.match("\"[^\"]+\"", part, re.M | re.I) \
                .group() \
                .strip('"')
            part_type = part.replace(part_name, '') \
                .strip('"') \
                .strip(':')

            self.schema[index] = (part_name, part_type)

            if part_type == 'Array':
                self.strategyIdMultiplierIndex = index
            else:
                self.dimension_id_indexes.append(index)

            index += 1

    def parse_output_line(self, line):
        _dataMap = {}
        _baseline_bid_data = {}
        logging.debug('line={}'.format(line))
        line_parts = line.split('\t')

        stratIdMultiplierArray = line_parts[self.strategyIdMultiplierIndex] \
                                        .split(',')

        for stratIdMultiplier in stratIdMultiplierArray:
            _strategy_id = stratIdMultiplier.split(':')[0]
            _multiplier = stratIdMultiplier.split(':')[1]
            _baseline_bid = stratIdMultiplier.split(':')[2]
            _baseline_bid_data[_strategy_id] = _baseline_bid
            _strategy_id_data = {}
            for _dim_index in self.dimension_id_indexes:
                _dimension_name = self.schema[_dim_index][0]
                _strategy_id_data[_dimension_name] = {line_parts[_dim_index]: _multiplier}

            _dataMap[_strategy_id] = _strategy_id_data

        return _dataMap, _baseline_bid_data

    def __init__(self, output_schema_file_location):
        f = fileutils.open_file(output_schema_file_location)
        str = f.readline().strip()
        self.construct_schema(str)

if __name__ == "__main__":
    line = """"site_domain_id2":Long	"strategy_id:bid_price_multiplier":Array"""
    o = output_schema("./output_type.tsv")
    o.construct_schema(line)



