#!/bin/bash
#rank $1
#nc $2
for((i=0;i<$2;i++))
{
	if((i%$2==$1))
	then
		python act_test.py < ./xact-files/$i.txt > /dev/null 2> ./xres/$i-res.txt &
	fi
}