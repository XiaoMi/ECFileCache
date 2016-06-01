date
#sudo iptables -A OUTPUT -d 10.237.37.16 -j REJECT
sudo iptables -A OUTPUT -d zk2.onebox.srv -j REJECT
sleep 30
date
sudo  iptables -D OUTPUT 1
