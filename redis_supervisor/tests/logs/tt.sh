sudo tc qdisc add dev eth0 root netem delay 5s
sleep 10
sudo tc qdisc delete dev eth0 root netem delay 5s

