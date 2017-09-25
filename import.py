from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

## TODO: Create immutable dict
default_parameters = {
    "keyspace": "warehouse",
    "strategy": "SimpleStrategy",
    "replication": 3,
    "consistency": "one"
}

def init(current_session=session, parameters={}):
    "Create keyspace, and possibly set CONSISTENCY"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # CQL Statements
    cql_create_keyspace = (
        "CREATE KEYSPACE {keyspace} "
        "WITH REPLICATION = {{ "
            "'class': '{strategy}', "
            "'replication_factor':'{replication}' }}"
    ).format(**default_params)
    cql_set_consistency = "CONSISTENCY {consistency}".format(**default_params)
    # Execute CQL Statement
    current_session.execute(cql_create_keyspace)
    # current_session.execute(cql_set_consistency)


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
        "); "
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
        "); "
        ).format(**default_params)
    cql_create_district = (
        "CREATE TABLE {keyspace}.district( "
            "w_id                       INT, "
            "d_id                       INT, "
            "d_name                     TEXT, "
            "d_street                   TEXT, "
            "d_city                     TEXT, "
            "d_state                    TEXT, "
            "d_zip                      TEXT, "
            "d_tax                      DOUBLE, "
            "d_ytd                      DOUBLE, "
            "d_next_o_id                INT, "
            "last_unfulfilled_order     INT, "
            "PRIMARY KEY ((w_id), d_id) "
        "); "
        ).format(**default_params)
    cql_create_order = (
        "CREATE TABLE {keyspace}.order( "
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
        "); "
        ).format(**default_params)
    cql_create_stockbywarehouse = (
        "CREATE TABLE {keyspace}.stock_by_warehouse( "
            "w_id                       INT, "
            "i_id                       INT, "
            "i_name                     TEXT, "
            "i_price                    DOUBLE, "
            "i_im_id                    INT, "
            "i_data                     TEXT, "
            "s_quantity                 INT, "
            "s_ytd                      INT, "
            "s_order_cnt                INT, "
            "s_remote_cnt               INT, "
            "s_dist_info                TEXT, "
            "s_data                     TEXT, "
            "PRIMARY KEY ((w_id), i_id) "
        "); "
        ).format(**default_params)
    cql_create_customerbybalance = (
        "CREATE MATERIALISED VIEW {keyspace}.customer_by_warehouse AS"
            "SELECT * FROM {keyspace}.customer "
            "WHERE w_id IS NOT NULL and d_id IS NOT NULL and "
                "c_id IS NOT NULL and c_balance IS NOT NULL "
            "PRIMARY KEY ((w_id), d_id, c_id, c_balance) "
        "; "
        ).format(**default_params)
    current_session.execute(cql_create_customer)
    current_session.execute(cql_create_warehouse)
    current_session.execute(cql_create_orderline)
    current_session.execute(cql_create_district)
    current_session.execute(cql_create_order)
    current_session.execute(cql_create_district)
    current_session.execute(cql_create_stockbywarehouse)
    current_session.execute(cql_create_customerbybalance)


def loading_data(current_session=session):
    "Upload data"
    #TODO
    pass

def cleanup(current_session=session, parameters={}):
    "Clean up by tearing down keyspace"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # CQL Statements
    cql_drop_keyspace = "DROP KEYSPACE {keyspace}".format(**default_params)
    # Execute CQL Statement
    current_session.execute(cql_drop_keyspace)
