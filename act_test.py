from cassandra.cluster import Cluster
import os
import sys

import subprocess

from transactions import payment_transaction, stock_level_transaction, popular_item_transaction, top_balance_transaction, new_order_transaction
from txn3 import txn3
from txn4 import txn4

def act_test(act_file):
	f=open(act_file)
	line = f.readline()

	while line:
		para=line.strip().split(",")
		if para[0]=="N":
			num_item=0
			new_order_c_id=int(para[1])
			new_order_w_id=int(para[2])
			new_order_d_id=int(para[3])
			new_order_m=int(para[4])
			new_order_line=list()

		elif para[0]=="P":
			payment_transaction(c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]), payment=float(para[4]), current_session=session)
		elif para[0]=="D":
			txn3(current_session=session, w_id=int(para[1]), carrier_id=int(para[2]))
		elif para[0]=="O":
			txn4(current_session=session, c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]))
		elif para[0]=="S":
			stock_level_transaction(w_id=int(para[1]),d_id=int(para[2]),T=int(para[3]), L=int(para[4]), current_session=session)
		elif para[0]=="I":
			popular_item_transaction(i="I", w_id=int(para[1]), d_id=int(para[2]),L=int(para[3]), current_session=session)
		elif para[0]=="T":
			top_balance_transaction(current_session=session)
		else: #order line in the order
			new_order_line.append([int(para[0]),int(para[1]),int(para[2])])
			num_item += 1
			if num_item == new_order_m:
				new_order_transaction(c_id=new_order_c_id, w_id=new_order_w_id, d_id=new_order_d_id,M=new_order_m, items=new_order_line, current_session=session)

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