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


def create_column_families(current_session=session, parameters):
    "Creates Column Families and Materialised View(s) using CQL"
    # TODO
    pass


def loading_data(current_session=session):
    "Upload data"
    #TODO
    pass

def cleanup(current_session=session):
    "Clean up by tearing down keyspace"
    default_params = default_parameters.copy()
    default_params.update(parameters)
    # CQL Statements
    cql_drop_keyspace = "DROP KEYSPACE {keyspace}".format(**default_params)
    # Execute CQL Statement
    current_session.execute(cql_drop_keyspace)
