while : 
do
	netstat -us | grep "packet receive err"
	sleep 1
done
