#!/bin/bash
x=1
while [ $x -gt 0 ]
do
	curl --proxy "http://pis2015001:9336118148@172.31.1.4:8080/" --connect-timeout 10 www.google.com > /dev/null
	sleep 100
	echo "$x  "
	x=$(($x+1))
done
