__author__ = 'cliu'

import os


def refresh_table(db, table):
    os.system('hive -S -e "use ' + db + '; msck repair table ' + table + '"')