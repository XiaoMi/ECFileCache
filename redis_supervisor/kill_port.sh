port=$1
ps -ef |grep redis_supervisor |grep "$1.conf" | awk '{print $2}' | xargs kill -9
ps -ef |grep redis-server |grep ":$1"| awk '{print $2}' | xargs kill
