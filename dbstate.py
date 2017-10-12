from cassandra.cluster import Cluster
import os

def test_state(current_session):
    wid_list = []
    distinct_wid = session.execute("SELECT DISTINCT w_id FROM warehouse")
    for result in distinct_wid:
        wid_list.append(result.w_id)
    w_ytd_sum=0.0
    for w_id in wid_list:
        w_row = current_session.execute(
                """
                SELECT w_ytd
                FROM warehouse
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
                FROM district
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
                FROM customer
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
                FROM orders
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
                FROM orderline
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
                FROM stock_by_warehouse
                WHERE w_id = %s
                """ ,
                (w_id,)
        )
        S_QUANTITY_sum+=ol_row[0][0]
        S_YTD_sum+=ol_row[0][1]
        S_ORDER_CNT_sum+=ol_row[0][2]
        S_REMOTE_CNT_sum+=ol_row[0][3]
    print "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock : %d , %f , %d , %d" % (S_QUANTITY_sum,S_YTD_sum,S_ORDER_CNT_sum,S_REMOTE_CNT_sum)
if __name__ == '__main__':
    current_directory = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(os.path.sep, current_directory, "config.conf")
    default_parameters = {}
    execfile(config_path, default_parameters)
    
    cluster = Cluster(contact_points=default_parameters['hosts'])
    session = cluster.connect("warehouse")
    
    test_state(session)