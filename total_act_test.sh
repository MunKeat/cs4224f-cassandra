#!/bin/bash
for((rank=0;rank<$1;rank++))
{
	python total_act_test.py $rank $1 40 > /dev/null 2> $rank-res.txt &
}