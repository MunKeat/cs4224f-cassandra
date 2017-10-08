import subprocess
import csv

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(
        username='cs4224f', password='tE3w8JyB')
# cluster = Cluster(contact_points=['192.168.48.244','192.168.48.245','192.168.48.246','192.168.48.247','192.168.48.248'] ,auth_provider=auth_provider)
cluster = Cluster()
session = cluster.connect()

## TODO: Create immutable dict
default_parameters = {
    "keyspace": "warehouse",
    "strategy": "SimpleStrategy",
    "replication": 3,
    "consistency": "one"
}

## TODO: Ensure it runs for all computer (i.e. no hardcoding of path)
cqlsh_path = "/home/stuproj/cs4224f/cassandra/bin/cqlsh"

def create_keyspace(current_session=session, parameters={}):
    "Create keyspace"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # CQL Statements
    cql_create_keyspace = (
        "CREATE KEYSPACE {keyspace} "
        "WITH REPLICATION = {{ "
            "'class': '{strategy}', "
            "'replication_factor':'{replication}' }}"
    ).format(**default_params)
    # Execute CQL Statement
    current_session.execute(cql_create_keyspace)

def set_consistency(current_session=session, parameters={}):
    "Set CONSISTENCY"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    cql_set_consistency = "CONSISTENCY {consistency}".format(**default_params)
    # Execute set consistency
    subprocess.call([cqlsh_path,"127.0.0.1","-e", cql_set_consistency])

def create_column_families(current_session=session, parameters={}):
    "Creates Column Families and Materialised View(s) using CQL"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # Create Customer Column Family
    cql_create_customer = (
        "CREATE TABLE {keyspace}.customer( "
            "w_id       INT, "
            "d_id       INT, "
            "c_id       INT, "
            "w_name     TEXT, "
            "w_street_1 TEXT, "
            "w_street_2 TEXT, "
            "w_tax      DOUBLE, "
            "d_name     TEXT, "
            "d_street_1 TEXT, "
            "d_street_2 TEXT, "
            "d_tax      DOUBLE, "
            "c_first    TEXT, "
            "c_middle   TEXT, "
            "c_last     TEXT, "
            "c_street_1 TEXT, "
            "c_street_2 TEXT, "
            "c_city     TEXT, "
            "c_state    TEXT, "
            "c_zip      TEXT, "
            "c_phone    TEXT, "
            "c_since    TIMESTAMP, "
            "c_credit   TEXT, "
            "c_credit_lim   DOUBLE, "
            "c_discount     DOUBLE, "
            "c_balance      DOUBLE, "
            "c_ytd_payment  DOUBLE, "
            "c_payment_cnt  INT, "
            "c_delivery_cnt INT, "
            "c_data         TEXT, "
            "last_order_id  INT, "
            "last_order_date    TIMESTAMP, "
            "last_order_carrier INT, "
            "PRIMARY KEY ((w_id), d_id, c_id) "
        ") WITH CLUSTERING ORDER BY (d_id DESC, c_id DESC); "
    ).format(**default_params)
    cql_create_warehouse = (
        "CREATE TABLE {keyspace}.warehouse( "
            "w_id       INT, "
            "w_name     TEXT, "
            "w_street   TEXT, "
            "w_city     TEXT, "
            "w_state    TEXT, "
            "w_zip      TEXT, "
            "w_tax      DOUBLE, "
            "w_ytd      DOUBLE, "
            "PRIMARY KEY (w_id) "
        "); "
        ).format(**default_params)
    cql_create_orderline = (
        "CREATE TABLE {keyspace}.orderline( "
            "w_id               INT, "
            "d_id               TEXT, "
            "o_id               TEXT, "
            "ol_number          TEXT, "
            "ol_i_id            TEXT, "
            "ol_i_name          TEXT, "
            "ol_delivery_d      TIMESTAMP, "
            "ol_amount          DOUBLE, "
            "ol_supply_w_id     INT, "
            "ol_quantity        INT, "
            "ol_dist_info       TEXT, "
            "PRIMARY KEY ((w_id), d_id, o_id, ol_number) "
        ") WITH CLUSTERING ORDER BY (d_id DESC, o_id DESC, ol_number ASC); "
        ).format(**default_params)
    cql_create_district = (
        "CREATE TABLE {keyspace}.district( "
            "w_id                       INT, "
            "d_id                       INT, "
            "d_name                     TEXT, "
            "d_street_1                 TEXT,"
            "d_street_2                 TEXT,"
            "d_city                     TEXT, "
            "d_state                    TEXT, "
            "d_zip                      TEXT, "
            "d_tax                      DOUBLE, "
            "d_ytd                      DOUBLE, "
            "d_next_o_id                INT, "
            "last_unfulfilled_order     INT, "
            "PRIMARY KEY ((w_id), d_id) "
        ") WITH CLUSTERING ORDER BY (d_id DESC); "
        ).format(**default_params)
    cql_create_order = (
        "CREATE TABLE {keyspace}.orders( "
            "w_id                       INT, "
            "d_id                       INT, "
            "o_id                       INT, "
            "c_id                       INT, "
            "o_carrier_id               INT, "
            "o_ol_cnt                   INT, "
            "o_all_local                INT, "
            "o_entry_d                  TIMESTAMP, "
            "c_first                    TEXT, "
            "c_middle                   TEXT, "
            "c_last                     TEXT, "
            "popular_item_id            INT, "
            "popular_item_name          TEXT, "
            "popular_item_qty           INT, "
            "ordered_items              SET<INT>, "
            "PRIMARY KEY ((w_id), d_id,o_id) "
        ")WITH CLUSTERING ORDER BY (d_id DESC, o_id ASC); "
        ).format(**default_params)
    cql_create_stockbywarehouse = (
        "CREATE TABLE {keyspace}.stock_by_warehouse( "
            "w_id                       INT, "
            "i_id                       INT, "
            "i_name                     TEXT, "
            "i_price                    DOUBLE, "
            "i_im_id                    INT, "
            "i_data                     TEXT, "
            "s_quantity                 DOUBLE, "
            "s_ytd                      DOUBLE, "
            "s_order_cnt                INT, "
            "s_remote_cnt               INT, "
            "s_dist_info_01             TEXT, "
            "s_dist_info_02             TEXT, "
            "s_dist_info_03             TEXT, "
            "s_dist_info_04             TEXT, "
            "s_dist_info_05             TEXT, "
            "s_dist_info_06             TEXT, "
            "s_dist_info_07             TEXT, "
            "s_dist_info_08             TEXT, "
            "s_dist_info_09             TEXT, "
            "s_dist_info_10             TEXT, "
            "s_data                     TEXT, "
            "PRIMARY KEY ((w_id), i_id) "
        ") WITH CLUSTERING ORDER BY (i_id DESC); "
        ).format(**default_params)
    cql_create_customerbybalance = (
        "CREATE MATERIALISED VIEW {keyspace}.customer_by_warehouse AS"
            "SELECT * FROM {keyspace}.customer "
            "WHERE w_id IS NOT NULL and d_id IS NOT NULL and "
                "c_id IS NOT NULL and c_balance IS NOT NULL "
            "PRIMARY KEY ((w_id), d_id, c_id, c_balance) "
            "WITH CLUSTERING ORDER BY (d_id DESC, c_id DESC, c_balance DESC )"
        "; "
        ).format(**default_params)
    current_session.execute(cql_create_customer)
    current_session.execute(cql_create_warehouse)
    current_session.execute(cql_create_orderline)
    current_session.execute(cql_create_district)
    current_session.execute(cql_create_order)
    current_session.execute(cql_create_stockbywarehouse)
    # current_session.execute(cql_create_customerbybalance)

def loading_data(current_session=session):
    "Upload data"
    #TODO
    subprocess.call("cqlsh -f import.cql",shell=True)
    #read w_id from csv into list
    with open('wid_list.csv', 'r') as fp_w:
        reader = csv.reader(fp_w, delimiter='\n')
        data_read = [row for row in reader]
        data_list = [row[0] for row in data_read]
        #wid_list in the format of: [1, 2, 3]
        wid_list = [int(row) for row in data_list]
    #read customer_id from csv into list
    with open('cid_list.csv', 'r') as fp_c:
        reader = csv.reader(fp_c, delimiter=',')
        data_read = [row for row in reader]
        #cid_list in the format of: [[1, 1, 1], [1, 1, 2], [1, 1, 3]]
        cid_list = [[int(row[0]), int(row[1]), int(row[2])] for row in data_read]

    ##fill order: popular_item info
    for w_id in wid_list:
        for d_id in range(1, 11):
            orders = current_session.execute(
                """
                SELECT o_id
                FROM  {keyspace}.orders
                WHERE w_id = %s
                AND d_id = %s;
                """,
                (w_id, d_id)
            )
            for order in orders:
                orderlines = current_session.execute(
                    """
                    SELECT * 
                    FROM  {keyspace}.orderline
                    WHERE w_id = %s
                    AND d_id = %s
                    AND o_id = %s;
                    """,
                    (w_id, d_id, order.o_id)
                )
                popular_item_id = None
                popular_item_qty = -1
                popular_item_name = None
                ordered_items = set()
                for orderline in orderlines:
                    if popular_item_qty < orderline.ol_quantity:
                        popular_item_qty = orderline.ol_quantity
                        popular_item_name = orderline.ol_i_name
                        popular_item_id = orderline.ol_i_id
                    ordered_items.add(orderline.ol_i_id)
                current_session.execute(
                    """
                    UPDATE  {keyspace}.order
                        SET popular_item_qty = %s AND popular_item_name = %s AND popular_item_id = %s AND ordered_items = %s
                        WHERE w_id = %s 
                        AND d_id = %s
                        AND o_id = %s;
                    """,
                    (popular_item_qty, popular_item_name, popular_item_id, ordered_items, w_id, d_id, order.o_id)
                )

    ##fill district: last_unfulfilled_id
    for w_id in wid_list:
        for d_id in range(1, 11):
            orders = current_session.execute(
                """
                SELECT o_id
                FROM  {keyspace}.orders
                WHERE w_id = %s
                AND d_id = %s
                LIMIT 1;
                """,
                (w_id, d_id)
            )
            if len(orders) == 0:
                continue
            current_session.execute(
                """
                UPDATE  {keyspace}.district
                    SET last_unfulfilled_order = %s
                    WHERE w_id = %s 
                    AND d_id = %s;
                """,
                (orders[0].o_id, w_id, d_id)
            )

    ##fill customer: last_order_id
    for customer in cid_list:
        orders = current_session.execute(
            """
            SELECT o_id
            FROM  {keyspace}.orders
            WHERE w_id = %s
            AND d_id = %s
            AND c_id = %s;
            """,
            (customer[0], customer[1], customer[2])
        )
        if len(orders) == 0:
            continue
        last_order_id = orders[len(orders) - 1].o_id
        current_session.execute(
            """
            UPDATE  {keyspace}.customer
                SET last_order_id = %s
                WHERE w_id = %s 
                AND d_id = %s
                AND c_id = %s;
            """,
            (last_order_id, customer[0], customer[1], customer[2])
        )


def cleanup(current_session=session, parameters={}):
    "Clean up by tearing down keyspace"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # CQL Statements
    cql_drop_keyspace = "DROP KEYSPACE IF EXISTS {keyspace}".format(**default_params)
    # Execute CQL Statement
    current_session.execute(cql_drop_keyspace)

if __name__ == '__main__':
    cleanup()
    create_keyspace()
    set_consistency()
    create_column_families()
