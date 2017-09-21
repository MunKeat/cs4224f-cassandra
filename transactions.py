from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

def top_balance_transaction(current_session=session):
    cql_select_customerbybalance = (
        "SELECT c_first, c_middle, c_last, c_balance, w_name, d_name "
        "FROM customerByBalance"
        "ORDER BY c_balance"
        "LIMIT 10"
    )
    rows = current_session.execute(cql_select_customerbybalance)
    # TODO: Process rows to output json
