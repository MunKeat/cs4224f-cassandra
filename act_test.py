from cassandra.cluster import Cluster
import os
import sys

import transactions

import time


def act_test(session):
	line = sys.stdin.readline()
	num_tran=0
	while line:
		para=line.strip().split(",")
		if para[0]=="N":
			num_item=0
			new_order_c_id=int(para[1])
			new_order_w_id=int(para[2])
			new_order_d_id=int(para[3])
			new_order_m=int(para[4])
			new_order_line=list()
			num_tran+=1
		elif para[0]=="P":
			a=transactions.payment_transaction(c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]), payment=float(para[4]), current_session=session)
			num_tran+=1
		elif para[0]=="D":
			a=transactions.delivery_transaction(current_session=session, w_id=int(para[1]), carrier_id=int(para[2]))
			num_tran+=1
		elif para[0]=="O":
			a=transactions.order_status_transaction(current_session=session, c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]))
			num_tran+=1
		elif para[0]=="S":
			a=transactions.stock_level_transaction(w_id=int(para[1]),d_id=int(para[2]),T=int(para[3]), L=int(para[4]), current_session=session)
			num_tran+=1
		elif para[0]=="I":
			a=transactions.popular_item_transaction(i="I", w_id=int(para[1]), d_id=int(para[2]),L=int(para[3]), current_session=session)
			num_tran+=1
		elif para[0]=="T":
			a=transactions.top_balance_transaction(current_session=session)
			num_tran+=1
		else: #order line in the order
			new_order_line.append([int(para[0]),int(para[1]),int(para[2])])
			num_item += 1
			if num_item == new_order_m:
				a=transactions.new_order_transaction(c_id=new_order_c_id, w_id=new_order_w_id, d_id=new_order_d_id,M=new_order_m, items=new_order_line, current_session=session)

		line = sys.stdin.readline()
	
	return num_tran

if __name__ == '__main__':

	current_directory = os.path.dirname(os.path.realpath(__file__))
	config_path = os.path.join(os.path.sep, current_directory, "config.conf")
	default_parameters = {}
	execfile(config_path, default_parameters)
	
	cluster = Cluster(contact_points=default_parameters['hosts'])
	session = cluster.connect("warehouse")
	session.default_consistency_level=default_parameters['consistency_level']
	
	num_tran=0
	before_time=time.time()
	
	num_tran=act_test(session)
	
	after_time=time.time()

	total_time=after_time-before_time

	sys.stderr.write("Total number of transactions processed:%d\n" % num_tran)
	sys.stderr.write("Total elapsed time for processing the transactions:%f\n" % total_time)
	sys.stderr.write("Transaction throughput:%f\n" % (num_tran/total_time))


'''You can use a new cqlsh command, CONSISTENCY, to set the consistency level for queries from the current cqlsh session.
 The WITH CONSISTENCY clause has been removed from CQL commands. 
 You set the consistency level programmatically (at the driver level). 
 For example, call QueryBuilder.insertInto with a setConsistencyLevel argument. 
 The consistency level defaults to ONE for all write and read operations.'''