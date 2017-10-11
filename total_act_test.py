import sys
import os
import subprocess
import thread
def swim_act(rank,nc,nact):
	for i in range(1,nact+1):
		if(i%nc==rank)
			subprocess.call("python act_test.py < %d.txt" % i,shell=True)
if __name__ == '__main__':
	rank=int(sys.argv[1])
	nc=int(sys.argv[2])
	nact=int(sys.argv[3])
	swim_act(rank,nc,nact)