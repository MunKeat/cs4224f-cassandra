import time

"""
by yuxin

changes: 
for part a), find out smallest order from order table instead of 
from District table (last_unfulfilled_order column)
since it will need more read and write
"""
def txn3(current_session, w_id, carrier_id):
    #TODO: validate carrier_id and w_id
    districts = current_session.execute(
        """
        SELECT * 
        FROM  {keyspace}.district
        WHERE w_id = %s;
        """,
        [w_id]
    )
    for district in districts:
        # a)
        orders = current_session.execute(
            """
            SELECT * 
            FROM  {keyspace}.order
            WHERE w_id = %s
            AND d_id = %s;
            """,
            (w_id, district.d_id)
        )
        o_id = None
        c_id = None
        for order in orders:
            if order.o_carrier_id is None:
                o_id = order.o_id
                c_id = order.c_id
                break
        if o_id is None:
            break
        # b)
        current_session.execute(
                    """
                    UPDATE  {keyspace}.order
                        SET o_carrier_id = %s
                        WHERE w_id = %s 
                        AND d_id = %s
                        AND o_id = %s;
                    """,
                    (carrier_id, w_id, district.d_id, o_id)
        )
        # C) update all order line: need to read order line number & amount first
        order_amt = 0.0
        orderlines = current_session.execute(
            """
            SELECT * 
            FROM  {keyspace}.orderline
            WHERE w_id = %s
            AND d_id = %s
            AND o_id = %s;
            """,
            (w_id, district.d_id, o_id)
        )
        timestamp = time.time()
        for orderline in orderlines:
            order_amt += orderline.ol_amount
            current_session.execute(
                """
                UPDATE  {keyspace}.orderline
                    SET ol_delivery_d = %s
                    WHERE w_id = %s 
                    AND d_id = %s
                    AND o_id = %s
                    AND ol_number = %s;
                """,
                (timestamp, w_id, district.d_id, o_id, orderline.ol_number)
            )
        # d) update customer table
        customers = current_session.execute(
            """
            SELECT * 
            FROM  {keyspace}.customer
            WHERE w_id = %s
            AND d_id = %s
            AND o_id = %s
            AND c_id = %s;
            """,
            (w_id, district.d_id, o_id, c_id)
        )
        customer = customers[0]
        current_session.execute(
                """
                UPDATE  {keyspace}.customer
                    SET c_balance = %s AND ol_amount = %s
                    WHERE w_id = %s 
                    AND d_id = %s
                    AND c_id = %s;
                """,
                (customer.c_balance + order_amt, w_id, district.d_id, c_id)
        )

