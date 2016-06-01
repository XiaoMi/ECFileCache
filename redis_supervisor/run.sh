#!/bin/bash

WORK_DIR=`dirname $0`
cd $WORK_DIR

exec python ./redis_supervisor/redis_supervisor.py -f ./conf/supervisor.conf
