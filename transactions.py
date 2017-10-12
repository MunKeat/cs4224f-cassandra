from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime

cluster = Cluster(contact_points=['192.168.48.244','192.168.48.245','192.168.48.246','192.168.48.247','192.168.48.248'])
session = cluster.connect('warehouse')

###############################################################################
#
# Utility Function(s)
#
###############################################################################
import pprint

def output(dictionary):
    output_form = "PRETTY_PRINT"
    if output_form == "RAW_PRINT":
        print(dictionary)
        return None
    else if output_form == "PRETTY_PRINT":
        pprint.pprint(dictionary)
        return None
    else:
        return dictionary

###############################################################################
#
# TRANSACTION 1
#
# Comment: Assume items is a list of items in the order
#
###############################################################################
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
    current_session.execute(
        """
        UPDATE district
        SET d_next_o_id = %(d_next_o_id)s
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'d_id': d_id, 'w_id': w_id, 'd_next_o_id': order_number + 1}
    )

    total_amount = 0.0
    all_item_id = set()
    ordered_items = {}
    popular_item_id = []
    popular_item_qty = 0
    popular_item_name = []
    isAllLocal = True

    # Prepared statements for order line transactions
    if (d_id == 10):
        s_dist_col_name = 's_dist_info_' + str(d_id)
    elif (d_id < 10):
        s_dist_col_name = 's_dist_info_0' + str(d_id)
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
        s_order_cnt = ?,
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
    batch = BatchStatement()
    # Create order line for each item in the order
    for ol_number in range(0, M):
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
        batch.add(update_stock_stmt, (adjusted_qty, stock_ytd_adjusted, s_order_cnt + 1, s_remote_cnt + int(isRemote), ol_supply_w_id, ol_i_id))
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
        "o_all_local": int(isAllLocal),
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
    batch.add(cql_create_order, order_info)
    ##current_session.execute(cql_create_order, order_info)
    # Execute batch operations
    session.execute(batch)

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

###############################################################################
#
# TRANSACTION 2
#
###############################################################################
def payment_transaction(c_w_id, c_d_id, c_id, payment, current_session=session):
    batch = BatchStatement()
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
    batch.add(
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
    batch.add(
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
    batch.add(
        """
        UPDATE district
        SET d_ytd = %(d_ytd)s
        WHERE w_id = %(w_id)s
        AND d_id = %(d_id)s
        """,
        {'w_id': c_w_id, 'd_id':c_d_id, 'd_ytd': d_ytd}
    )
    current_session.execute(batch)
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
            AND d_id = %s
            ORDER BY d_id, o_id ASC;
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
                    SET c_balance = %s, c_delivery_cnt = %s
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
    orders = current_session.execute(
        """
        SELECT * 
        FROM  orders
        WHERE w_id = %s
        AND d_id = %s;
        """,
        (c_w_id, c_d_id)
    )
    last_order_id = None
    last_order_date = None
    last_order_carrier = None
    for order in orders:
        if order.c_id == c_id:
            last_order_id = order.o_id
            last_order_date = order.o_entry_d
            last_order_carrier = order.o_carrier_id
            break
    if last_order_id is None:
        return output
    output['o_id'] = last_order_id
    output['o_entry_d'] = last_order_date
    output['o_carrier_id'] = last_order_carrier
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

###############################################################################
#
# TRANSACTION 5
#
# Comment: WIP
#
###############################################################################
def stock_level_transaction(w_id, d_id,T, L, current_session=session):
    parameters = {
        "w_id": w_id,
        "d_id": d_id,
        "T": T,
        "l": L
    }
    cql_select_order = (
        "SELECT w_id, d_id, o_id,ordered_items "
        "FROM orders "
        "WHERE w_id = %(w_id)s AND d_id = %(d_id)s "
        #"ORDER BY d_id,o_id DESC "
        "LIMIT %(l)s"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)
    all_item_id = set()
    for row in rows:
        all_item_id = all_item_id | row.ordered_items
    parameters["all_item_id"]=str(tuple(all_item_id))
    cql_select_order = (
        "SELECT w_id, i_id, i_name "
        "FROM stock_by_warehouse "
        "WHERE w_id = %(w_id)s AND i_id IN "+parameters["all_item_id"]+ " AND s_quantity < %(T)s "
        "ALLOW FILTERING"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)

    st_count=0
    for row in rows:
        st_count+=1
    return st_count


###############################################################################
#
# TRANSACTION 6
#
###############################################################################
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
        "WHERE w_id=%(w_id)s AND d_id=%(d_id)s "
        #"ORDER BY o_id DESC "
        "LIMIT %(l)s"
    )
    output_1 = []
    output_2 = []
    rows = current_session.execute(cql_select_order, parameters=parameters)
    number_of_orders = 0
    popular_item_id = []
    popular_item_name = []
    order_item_id = []
    for row in rows:
        number_of_orders += 1
        popular_quantity = row.popular_item_qty
        popular_items = [{'i_name': name, 'ol_quantity': popular_quantity} for name in popular_item_name]
        output_1.append({'w_id': row.w_id, 'd_id': row.d_id, 'o_id': row.o_id,
                         'o_entry_d': row.o_entry_d, 'c_first': row.c_first,
                         'c_middle': row.c_middle, 'c_last': row.c_last, 'popular_items': popular_items})
        popular_item_id.extend(row.popular_item_id)
        popular_item_name.extend(row.popular_item_name)
        order_item_id.append(row.ordered_items)
    # Get distinct popular items
    distinct_popular_item = list(set([tuple([id, name]) for id, name in zip(popular_item_id, popular_item_name)]))
    # Perform percentage count
    raw_count = [[(item_id in single_ordered_items)
                  for single_ordered_items in order_item_id].count(True)
                    for item_id, item_name in distinct_popular_item]
    output_2 = [{"i_name": item[1], "percentage": float(item_count) / number_of_orders} for item, item_count in zip(distinct_popular_item, raw_count)]
    output(output_1)
    output(output_2)
    # return output_1
    # return output_2

###############################################################################
#
# TRANSACTION 7
#
###############################################################################
def top_balance_transaction(current_session=session):
    # TODO: Move this out
    list_of_distinct_wid = []
    distinct_wid = session.execute("SELECT DISTINCT w_id FROM warehouse")
    for result in distinct_wid:
        list_of_distinct_wid.append(result.w_id)
    # Begin transaction
    output = []
    highest_balance = []
    for id in list_of_distinct_wid:
        cql_select_customerbybalance = (
            "SELECT c_first, c_middle, c_last, c_balance, w_name, d_name "
            "FROM customer_by_balance "
            "WHERE w_id = {} "
            "ORDER BY c_balance DESC "
            "LIMIT 10".format(id)
        )
        rows = current_session.execute(cql_select_customerbybalance)
        for row in rows:
            highest_balance.append(row)
    # highest_balance.sort(key=lambda x: float(x.c_balance), reverse=True)
    highest_balance = sorted(highest_balance, key=lambda x:float(x.c_balance), reverse=True)
    highest_balance = highest_balance[:10]
    for customer in highest_balance:
        output.append({'c_first': customer.c_first, 'c_middle': customer.c_middle, 'c_last': customer.c_last, 'c_balance': customer.c_balance, 'w_name':customer.w_name, 'd_name': customer.d_name})
    output(output)
