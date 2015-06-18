ps -ef |grep redis_supervisor  |awk '{print $2}' | xargs kill -9
killall redis-server
