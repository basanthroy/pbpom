#!/bin/sh

if ps ax | grep -v grep | grep "pom_pb_dq.py" > /dev/null
then
    date >> /opt/dwradiumone/r1-dw-arte-app/test/log/cron.log
    echo "previous pb dequeue run for prod not done yet. exit ..." >> /opt/dwradiumone/r1-dw-arte-app/test/log/cron.log
else
    date >> /opt/dwradiumone/r1-dw-arte-app/test/log/cron.log
    echo "previous pb dequeue run for prod completed. Starting a new one ..." >> /opt/dwradiumone/r1-dw-arte-app/test/log/cron.log
    export PYTHONPATH="/opt/dwradiumone/r1-dw-arte-app/test/scripts/src/main/python"
    /opt/python2.7/bin/python /opt/dwradiumone/r1-dw-arte-app/test/scripts/src/main/python/pb/nqdq/pom_pb_dq.py
fi
