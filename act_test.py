from cassandra.cluster import Cluster
import os
import sys

import subprocess

def act_test(act_file):
	f=open(act_file)
	line = f.readline()
	order_list=[]
	while line:"O, S, I, or T""
		para=line.strip().split(",")
		if para[0]=="N":
			pass
		elif para[0]=="P":
			pass
		elif para[0]=="D":
			pass
		elif para[0]=="O":
			pass
		elif para[0]=="S":
			pass
		elif para[0]=="I":
			pass
		elif para[0]=="T":
			pass
		else: #order line in the order
			pass

		line = f.readline()
		

	f.close()

if __name__ == '__main__':
	for i in xrange(1,41):
		act_test("%d.txt" % i)




'''You can use a new cqlsh command, CONSISTENCY, to set the consistency level for queries from the current cqlsh session.
 The WITH CONSISTENCY clause has been removed from CQL commands. 
 You set the consistency level programmatically (at the driver level). 
 For example, call QueryBuilder.insertInto with a setConsistencyLevel argument. 
 The consistency level defaults to ONE for all write and read operations.'''