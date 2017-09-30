"""
by yuxin
"""
def txn4(current_session, c_w_id, c_d_id, c_id):
    output = {}
    customer = current_session.execute(
        """
        SELECT * 
        FROM  {keyspace}.customer
        WHERE c_w_id = %s
        AND c_d_id = %s
        AND c_id = %s;
        """,
        (c_w_id, c_d_id, c_id)
    )
    #1) out put customer information
    if len(customer) != 1:
        return output
    output['c_first'] = customer[0].c_first
    output['c_middle'] = customer[0].c_middle
    output['c_last'] = customer[0].c_last
    output['c_balance'] = customer[0].c_balance
    #2) get customer's last order
    output['o_id'] = customer[0].last_oid
    output['o_entry_d'] = customer[0].last_o_entry_d
    output['o_carrier_id'] = customer[0].last_o_carrier_id
    #3) each item information
    orderlines = current_session.execute(
        """
        SELECT * 
        FROM  {keyspace}.orderline
        WHERE w_id = %s
        AND d_id = %s
        AND o_id = %s;
        """,
        (c_w_id, c_d_id, output['o_id'])
    )
    items = {}
    for orderline in orderlines:
        items[orderline.ol_number] = {}
        items[orderline.ol_number]['ol_i_id'] = orderline.ol_i_id
        items[orderline.ol_number]['ol_supply_w_id'] = orderline.ol_supply_w_id
        items[orderline.ol_number]['ol_quantity'] = orderline.ol_quantity
        items[orderline.ol_number]['ol_amount'] = orderline.ol_amount
        items[orderline.ol_number]['ol_delivery_id'] = orderline.ol_delivery_id
    output['items'] = items
    return output
