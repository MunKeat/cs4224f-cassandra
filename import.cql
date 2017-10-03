-- Customer COPY
COPY warehouse.customer (
    w_id , d_id , c_id, w_name, w_street_1, w_street_2, w_tax, d_name,
    d_street_1, d_street_2, d_tax, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
    c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data,
    last_order_id, last_order_date, last_order_carrier )
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_customer.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';

-- Warehouse COPY
COPY warehouse.warehouse (w_id, w_name, w_street, w_city, w_state, w_zip, w_tax, w_ytd )
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_warehouse.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';

-- Order COPY
COPY warehouse.orders(
w_id, d_id, o_id, c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d,
c_first, c_middle, c_last, popular_item_id, popular_item_name, popular_item_qty, ordered_items)
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_order.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';

-- Stock By Warehouse COPY
COPY warehouse.stock_by_warehouse (w_id, i_id, i_name, i_price, i_im_id, i_data, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_info_01, s_dist_info_02, s_dist_info_03, s_dist_info_04, s_dist_info_05, s_dist_info_06, s_dist_info_07, s_dist_info_08, s_dist_info_09, s_dist_info_10,  s_data)
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_stockitem.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';

-- District COPY
COPY warehouse.district(w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id, last_unfulfilled_order)
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_district.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';

-- Orderline COPY
COPY warehouse.orderline(w_id, d_id, o_id, ol_number, ol_i_id, ol_i_name, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
FROM '/home/stuproj/cs4224f/cassandra-benchmark/data/cassandra_order-line.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='null';






