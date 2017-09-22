from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

# Current WIP - Not proven to work
def popular_item_transaction(i, w_id, d_id, L, current_session=session):
    parameters = {
        "i": i,
        "w_id": w_id,
        "d_id": d_id,
        "l": L
    }
    cql_select_order = (
        "SELECT w_id, d_id, o_id. o_entry_d. c_first, c_middle, c_last, "
        "popular_item_id, popular_item_name, popular_item_qty, ordered_items "
        "FROM order "
        "WHERE w_id %(w_id)s AND d_id %(d_id)s "
        "ORDER BY o_id DESC "
        "LIMIT %(l)s"
    )
    rows = current_session.execute(cql_select_order, parameters=parameters)
    number_of_entries = length(rows)
    # Get distinct popular items
    distinct_popular_item = list(set([(row.popular_item_name, int(row.popular_item_id)) for row in rows]))s
    # Get a list of ordered items
    ordered_items = ([list(row.ordered_items) for row in rows])
    # Perform percentage count
    raw_count = ([(item_id in single_ordered_items).count(True)
                  for single_ordered_items in ordered_items]
                    for item_id, item_name in distinct_popular_item)
    output = [item, float(item_count) / number_of_entries
                for item, item_count in zip(distinct_popular_item, raw_count)]
    # TODO: Process rows to output json


# Current WIP - Not proven to work
def top_balance_transaction(current_session=session):
    cql_select_customerbybalance = (
        "SELECT c_first, c_middle, c_last, c_balance, w_name, d_name "
        "FROM customerByBalance"
        "ORDER BY c_balance DESC "
        "LIMIT 10"
    )
    rows = current_session.execute(cql_select_customerbybalance)
    # TODO: Process rows to output json
