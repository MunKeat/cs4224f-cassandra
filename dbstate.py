from cassandra.cluster import Cluster

import csv

def test_state():
	#read w_id from csv into list
    with open('wid_list.csv', 'r') as fp_w:
        reader = csv.reader(fp_w, delimiter='\n')
        data_read = [row for row in reader]
        data_list = [row[0] for row in data_read]
        #wid_list in the format of: [1, 2, 3]
        wid_list = [int(row) for row in data_list]
    w_ytd_sum=0.0
    for w_id in wid_list:
    	w_row = current_session.execute(
                """
                SELECT w_ytd
        		FROM {keyspace}.warehouse
       			WHERE w_id = %s
                """,
                (w_id,)
        )
        w_ytd_sum+=w_row[0].w_ytd
    
    print "select sum(W_YTD) from Warehouse : %f" % w_ytd_sum

    d_ytd_sum=0.0
    d_next_o_id_sum=0
    for w_id in wid_list:
        d_row = current_session.execute(
                """
                SELECT sum(d_ytd), sum(d_next_o_id)
        		FROM {keyspace}.district
       			WHERE w_id = %s
                """ ,
                (w_id,)
        )
        d_ytd_sum+=d_row[0][0]
        d_next_o_id_sum+=d_row[0][1]

   	print "select sum(D_YTD), sum(D_NEXT_O_ID) from District : %f , %d" % (d_ytd_sum,d_next_o_id_sum)

   	C_BALANCE_sum=0.0
   	C_YTD_PAYMENT_sum=0.0
    C_PAYMENT_CNT_sum=0
    C_DELIVERY_CNT_sum=0
    for w_id in wid_list:
        c_row = current_session.execute(
                """
                SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt)
        		FROM {keyspace}.customer
       			WHERE w_id = %s
                """ ,
                (w_id,)
        )
        C_BALANCE_sum+=c_row[0][0]
	   	C_YTD_PAYMENT_sum+=c_row[0][1]
	    C_PAYMENT_CNT_sum+=c_row[0][2]
	    C_DELIVERY_CNT_sum+=c_row[0][3]
    print "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer: %f , %f , %d , %d" % (C_BALANCE_sum,C_YTD_PAYMENT_sum,C_PAYMENT_CNT_sum,C_DELIVERY_CNT_sum)

    O_ID_max=0
    O_OL_CNT_sum=0
    for w_id in wid_list:
        o_row = current_session.execute(
                """
                SELECT max(o_id), sum(o_ol_cnt)
        		FROM {keyspace}.orders
       			WHERE w_id = %s
                """ ,
                (w_id,)
        )
        O_ID_max= max(o_row[0][0],O_ID_max)
	   	O_OL_CNT_sum+=o_row[0][1]
	print "select max(O_ID), sum(O_OL_CNT) from Order : %d , %d" % (O_ID_max,O_OL_CNT_sum) 

	OL_AMOUNT_sum=0.0
	OL_QUANTITY_sum=0
	for w_id in wid_list:
        ol_row = current_session.execute(
                """
                SELECT sum(ol_amount), sum(ol_quantity)
        		FROM {keyspace}.orderline
       			WHERE w_id = %s
                """ ,
                (w_id,)
        )
        OL_AMOUNT_sum += ol_row[0][0]
	   	OL_QUANTITY_sum += ol_row[0][1]
	print "select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order-Line : %f , %d" % (OL_AMOUNT_sum,OL_QUANTITY_sum) 

	
	S_QUANTITY_sum=0
	S_YTD_sum=0.0
	S_ORDER_CNT_sum=0
	S_REMOTE_CNT_sum=0
	for w_id in wid_list:
        ol_row = current_session.execute(
                """
                SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt)
        		FROM {keyspace}.stockbywarehouse
       			WHERE w_id = %s
                """ ,
                (w_id,)
        )
        OL_AMOUNT_sum += ol_row[0][0]
	   	OL_QUANTITY_sum += ol_row[0][1]
	print "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock : %d , %f , %d , %d" % (S_QUANTITY_sum,S_YTD_sum,S_ORDER_CNT_sum,S_REMOTE_CNT_sum)
if __name__ == '__main__':
	test_state()