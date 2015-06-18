REDIS_CONF_PATH="/opt/soft/redis/conf"
for port in $(seq 6379 6391)
do
    echo "start redis of port: $port"
    sed "s/supervisor/&_${port}/" ./conf/log4p.conf.bak > ./conf/log4p.conf
    NEW_REDIS_CONF_FILE=${REDIS_CONF_PATH}/redis_${port}.conf
    sed "s/^port 6379/port ${port}/" ${REDIS_CONF_PATH}/sample_redis.conf  > ${NEW_REDIS_CONF_FILE} 
    python ./redis_supervisor/redis_supervisor.py -f ./conf/supervisor.conf -p $port -c ${NEW_REDIS_CONF_FILE} &
    sleep 1
done
cp ./conf/log4p.conf.bak ./conf/log4p.conf
