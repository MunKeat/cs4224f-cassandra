from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster(contact_points=['192.168.48.244','192.168.48.245','192.168.48.246','192.168.48.247','192.168.48.248'])
session = cluster.connect('warehouse')

# Current WIP - Not proven to work
# assuming items is a list of items in the order
# Transaction 1
def new_order_transaction(c_id, w_id, d_id, M, items, current_session=session):
    num_of_items = M

    # Retrieve customer info
    customers = current_session.execute(
        """
        SELECT c_last, c_first, c_middle, c_credit, c_discount
        FROM customer
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        AND c_id = %(c_id)s
        """,
        {'d_id': d_id, 'w_id': w_id, 'c_id': c_id}
    )
    customer = customers[0]
    c_last = customer.c_last
    c_middle = customer.c_middle
    c_first = customer.c_first
    c_credit = customer.c_credit
    c_discount = customer.c_discount
    # Retrieve warehouse info
    warehouses = current_session.execute(
        """
        SELECT w_tax
        FROM warehouse
        WHERE w_id = %(w_id)s
        """,
        {'w_id': w_id}
    )
    w_tax = warehouses[0].w_tax
    # Retrieve district info
    districts = current_session.execute(
        """
        SELECT d_tax, d_next_o_id
        FROM district
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'d_id': d_id, 'w_id': w_id}
    )
    district = districts[0]
    d_tax = district.d_tax
    order_number = district.d_next_o_id
    # Update next available order number
    cql_update_order_number = (
        """
        UPDATE district
        SET d_next_o_id = d_next_o_id + 1
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'d_id': d_id, 'w_id': w_id}
    )
    current_session.execute(cql_update_order_number)

    total_amount = 0.0
    all_item_id = set()
    ordered_items = {}
    popular_item_id = []
    popular_item_qty = 0
    popular_item_name = []
    isAllLocal = True

    # Prepared statements for order line transactions
    if (d_id == 10):
        s_dist_col_name = 's_dist_info_' + d_id
    elif (d_id < 10):
        s_dist_col_name = 's_dist_info_0' + d_id
    get_item_stock_stmt = session.prepare(
        """
        SELECT s_quantity, s_ytd, {} AS s_dist_info, s_order_cnt, s_remote_cnt, i_name, i_price
        FROM stock_by_warehouse
        WHERE w_id = ?
        AND i_id = ?
        """.format(s_dist_col_name)
    )
    update_stock_stmt = session.prepare(
        """
        UPDATE stock_by_warehouse
        SET s_quantity = ?,
        s_ytd = ?,
        s_order_cnt = ?
        s_remote_cnt = ?
        WHERE w_id = ?
        AND i_id = ?
        """
    )
    create_ol_stmt = session.prepare(
        """
        INSERT INTO orderline
        (w_id, d_id, o_id, ol_number, ol_i_id, ol_i_name, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    # Create order line for each item in the order
    for ol_number in range(0, M):
        batch = BatchStatement()
        item = items[ol_number]
        ol_i_id = item[0]
        ol_supply_w_id = item[1]
        ol_quantity = item[2]
        # Retrieve stock and item info
        item_stocks = current_session.execute(get_item_stock_stmt, [ol_supply_w_id, ol_i_id])
        item_stock = item_stocks[0]
        stock_qty = item_stock.s_quantity
        stock_ytd = item_stock.s_ytd
        ol_dist_info = item_stock.s_dist_info
        item_price = item_stock.i_price
        item_name = item_stock.i_name
        s_order_cnt = item_stock.s_order_cnt
        s_remote_cnt = item_stock.s_remote_cnt
        item_amount = ol_quantity * item_price
        total_amount += item_amount
        # Update stock
        adjusted_qty = stock_qty - ol_quantity
        if (adjusted_qty < 10):
            adjusted_qty += 100
        stock_ytd_adjusted = stock_ytd + ol_quantity
        isRemote = (w_id!=ol_supply_w_id)
        if isRemote:
            isAllLocal = False
        batch.add(update_stock_stmt, (adjusted_qty, stock_ytd_adjusted, s_order_cnt + 1, s_remote_cnt + isRemote, ol_supply_w_id, ol_i_id))
        # Update popular item
        if (ol_quantity > popular_item_qty):
            popular_item_qty = ol_quantity
            popular_item_id = [ol_i_id]
            popular_item_name = [item_name]
        elif (ol_quantity == popular_item_qty):
            popular_item_id.append(ol_i_id)
            popular_item_name.append(item_name)

        # Create a new order line
        ol_info = (w_id, d_id, order_number, ol_number, ol_i_id, item_name, item_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        batch.add(create_ol_stmt, ol_info)
        # Execute batch operations
        session.execute(batch)
        # Update other info for output
        ordered_item = {
                'item_number': ol_i_id, 
                'i_name': item_name, 
                'supplier_warehouse': ol_supply_w_id,
                'quantity': ol_quantity,
                'ol_amount': item_amount,
                's_quantity': adjusted_qty
        }
        ordered_items[ol_number] = ordered_item
        all_item_id.add(ol_i_id)

    # Compute total amount after tax and discount
    total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
    # Create a new order
    o_entry_d = datetime.utcnow()
    order_info = {
        'w_id': w_id,
        'd_id': d_id,
        "o_id": order_number,
        "c_id": c_id,
        "o_ol_cnt": M,
        "o_all_loca": int(isAllLocal),
        "o_entry_d": o_entry_d,
        "c_first": c_first,
        "c_middle": c_middle,
        "c_last": c_last,
        "popular_item_id": popular_item_id,
        "popular_item_name": popular_item_name,
        "popular_item_qty": popular_item_qty,
        "ordered_items": all_item_id
    }
    cql_create_order = (
        """
        INSERT INTO orders
        (w_id, d_id, o_id, c_id, 
            o_ol_cnt, o_all_local, o_entry_d, 
            c_first, c_middle, c_last, 
            popular_item_id, popular_item_name, popular_item_qty,
            ordered_items)
        VALUES 
        (%(w_id)s, %(d_id)s, %(o_id)s, %(c_id)s, 
            %(o_ol_cnt)s, %(o_all_local)s, %(o_entry_d)s,
            %(c_first)s, %(c_middle)s, %(c_last)s, 
            %(popular_item_id)s, %(popular_item_name)s, %(popular_item_qty)s,
            %(ordered_items)s)
        """
    )
    current_session.execute(cql_create_order, order_info)

    output = {
        'w_id': w_id,
        'd_id': d_id,
        'c_id': c_id,
        'c_last': c_last,
        'c_credit': c_credit,
        'c_discount': c_discount,
        'w_tax': w_tax,
        'd_tax': d_tax,
        'o_id': order_number,
        'o_entry_d': o_entry_d,
        'num_items': M,
        'total_amound': total_amount,
        'ordered_item': ordered_items
    }
    return output

# Current WIP - Not proven to work
# Transaction 2
def payment_transaction(c_w_id, c_d_id, c_id, payment, current_session=session):
    # Retrieve customer information
    customers = current_session.execute(
        """
        SELECT *
        FROM customer
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        AND c_id = %(c_id)s
        """,
        {'d_id': c_d_id, 'w_id': c_w_id, 'c_id': c_id}
    )
    customer = customers[0]
    c_balance = customer.c_balance
    c_ytd_payment = customer.c_ytd_payment
    c_payment_cnt = customer.c_payment_cnt
    # Update customer
    c_balance -= payment
    c_ytd_payment += payment
    current_session.execute(
        """
        UPDATE customer
        SET c_balance = %(c_balance)s,
        c_ytd_payment = %(c_ytd_payment)s,
        c_payment_cnt = %(c_payment_cnt)s
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        AND c_id = %(c_id)s
        """,
        {'c_balance': c_balance, 'c_ytd_payment': c_ytd_payment,'c_payment_cnt': c_payment_cnt + 1, 'w_id': c_w_id, 'd_id': c_d_id, 'c_id': c_id}
    )
    # Retrieve warehouse information
    warehouses = current_session.execute(
        """
        SELECT *
        FROM warehouse
        WHERE w_id = %(w_id)s
        """,
        {'w_id': c_w_id}
    )
    warehouse = warehouses[0]
    w_ytd = warehouse.w_ytd
    # Update warehouse
    w_ytd += payment
    current_session.execute(
        """
        UPDATE warehouse
        SET w_ytd = %(w_ytd)s
        WHERE w_id = %(w_id)s
        """,
        {'w_id': c_w_id, 'w_ytd': w_ytd}
    )
    # Retrieve district information
    districts = current_session.execute(
        """
        SELECT *
        FROM district
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'d_id': c_d_id, 'w_id': c_w_id}
    )
    district = districts[0]
    d_ytd = district.d_ytd
    # Update district
    d_ytd += payment
    current_session.execute(
        """
        UPDATE district
        SET d_ytd = %(d_ytd)s
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'w_id': c_w_id, 'd_id':c_d_id, 'w_ytd': w_ytd}
    )
    output = {
        'customer': (c_w_id, c_d_id, c_id),
        'customer_name': (customer.c_first, customer.c_middle, customer. c_last),
        'customer_address': (customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip),
        'c_phone': customer.c_phone,
        'c_since': customer.c_since,
        'c_credit': customer.c_credit,
        'c_credit_lim': customer.c_credit_lim,
        'c_discount': customer.c_discount,
        'c_balance': c_balance,
        'warehouse_address': (warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip),
        'district_address': (district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip),
        'payment': payment
    }
    return output

# Current WIP - Not proven to work
# Transaction 3
def delivery_transaction(w_id, carrier_id, current_session=session):
    #TODO: validate carrier_id and w_id
    for d_id in range(1, 11):
        # a)
        orders = current_session.execute(
            """
            SELECT * 
            FROM  orders
            WHERE w_id = %s
            AND d_id = %s;
            """,
            (w_id, d_id)
        )
        o_id = None
        c_id = None
        for order in orders:
            if order.o_carrier_id is None:
                o_id = order.o_id
                c_id = order.c_id
                break
        if o_id is None:
            continue
        # b)
        current_session.execute(
                    """
                    UPDATE  orders
                        SET o_carrier_id = %s
                        WHERE w_id = %s 
                        AND d_id = %s
                        AND o_id = %s;
                    """,
                    (carrier_id, w_id, d_id, o_id)
        )
        # C) update all order line: need to read order line number & amount first
        order_amt = 0.0
        orderlines = current_session.execute(
            """
            SELECT * 
            FROM  orderline
            WHERE w_id = %s
            AND d_id = %s
            AND o_id = %s;
            """,
            (w_id, d_id, o_id)
        )
        timestamp = datetime.utcnow()
        for orderline in orderlines:
            order_amt += orderline.ol_amount
            current_session.execute(
                """
                UPDATE  orderline
                    SET ol_delivery_d = %s
                    WHERE w_id = %s 
                    AND d_id = %s
                    AND o_id = %s
                    AND ol_number = %s;
                """,
                (timestamp, w_id, d_id, o_id, orderline.ol_number)
            )
        # d) update customer table
        customers = current_session.execute(
            """
            SELECT * 
            FROM  customer
            WHERE w_id = %s
            AND d_id = %s
            AND c_id = %s;
            """,
            (w_id, d_id, c_id)
        )
        customer = customers[0]
        current_session.execute(
                """
                UPDATE  customer
                    SET c_balance = %s AND c_delivery_cnt = %s
                    WHERE w_id = %s 
                    AND d_id = %s
                    AND c_id = %s;
                """,
                (customer.c_balance + order_amt, customer.c_delivery_cnt + 1, w_id, d_id, c_id)
        )

# Current WIP - Not proven to work
# Transaction 4
def order_status_transaction(c_w_id, c_d_id, c_id, current_session = session):
    output = {}
    customer = current_session.execute(
        """
        SELECT * 
        FROM  customer
        WHERE w_id = %s
        AND d_id = %s
        AND c_id = %s;
        """,
        (c_w_id, c_d_id, c_id)
    )
    #1) out put customer information
    if not customer:
        return output
    output['c_first'] = customer[0].c_first
    output['c_middle'] = customer[0].c_middle
    output['c_last'] = customer[0].c_last
    output['c_balance'] = customer[0].c_balance
    #2) get customer's last order
    output['o_id'] = customer[0].last_order_id
    output['o_entry_d'] = customer[0].last_order_date
    output['o_carrier_id'] = customer[0].last_order_carrier
    #3) each item information
    if output['o_id'] is None:
        #customer does not have any order yet
        return output
    orderlines = current_session.execute(
        """
        SELECT * 
        FROM  orderline
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
        items[orderline.ol_number]['ol_delivery_d'] = orderline.ol_delivery_d
    output['items'] = items
    return output

# Current WIP - Not proven to work
# Transaction 5
def stock_level_transaction(w_id, d_id,T, L, current_session=session):
    parameters = {
        "w_id": w_id,
        "d_id": d_id,
        "T": T,
        "l": L
    }
    cql_select_order = (
        "SELECT w_id, d_id, o_id,ordered_items"
        "FROM orders "
        "WHERE w_id = %(w_id)s AND d_id = %(d_id)s "
        "ORDER BY o_id DESC "
        "LIMIT %(l)s"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)
    number_of_entries = len(rows)
    all_item_id = set()
    for row in rows:
        all_item_id = all_item_id | row.ordered_items
    parameters["all_item_id"]=all_item_id
    cql_select_order = (
        "SELECT w_id, i_id, i_name"
        "FROM stock_by_warehouse "
        "WHERE w_id = %(w_id)s AND i_id IN %(d_id)s AND s_quantitiy < %(T)s"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)
    
    #output = list(row.i_name for row in rows)
    #output = [item, float(item_count) / number_of_entries for item, item_count in zip(distinct_popular_item, raw_count)]
    # TODO: Process rows to output json


# Current WIP - Not proven to work
# Transaction 6
def popular_item_transaction(i, w_id, d_id, L, current_session=session):
    parameters = {
        "i": i,
        "w_id": w_id,
        "d_id": d_id,
        "l": L
    }
    cql_select_order = (
        "SELECT w_id, d_id, o_id, o_entry_d, c_first, c_middle, c_last, "
        "popular_item_id, popular_item_name, popular_item_qty, ordered_items "
        "FROM orders "
        "WHERE w_id %(w_id)s AND d_id %(d_id)s "
        "ORDER BY o_id DESC "
        "LIMIT %(l)s"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)
    number_of_entries = len(rows)
    # Get distinct popular items
    distinct_popular_item = list(set([(row.popular_item_name, int(row.popular_item_id)) for row in rows]))
    # Get a list of ordered items
    ordered_items = ([list(row.ordered_items) for row in rows])
    # Perform percentage count
    raw_count = ([(item_id in single_ordered_items).count(True)
                  for single_ordered_items in ordered_items]
                    for item_id, item_name in distinct_popular_item)
    #output = [item, float(item_count) / number_of_entries for item, item_count in zip(distinct_popular_item, raw_count)]
    # TODO: Process rows to output json


# Current WIP - Not proven to work
# Transaction 7
def top_balance_transaction(current_session=session):
    cql_select_customerbybalance = (
        "SELECT c_first, c_middle, c_last, c_balance, w_name, d_name "
        "FROM customerByBalance"
        "ORDER BY c_balance DESC "
        "LIMIT 10"
    )
    rows = current_session.execute(cql_select_customerbybalance)
    # TODO: Process rows to output json
