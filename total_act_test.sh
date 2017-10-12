#!/bin/bash
#rank $1
#nc $2
for((i=0;i<$2;i++))
{
	python act_test.py < ./$i.txt > /dev/null 2> ./res/$i-res.txt &
}