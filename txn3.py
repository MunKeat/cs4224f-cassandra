import default_parameters from import.py

def txn3(current_session, w_id, carrier_id):
	#TODO: validate carrier_id and w_id
    districts = session.execute(
	    """
	    SELECT * 
	    FROM  {keyspace}.district
	    WHERE w_id = %s;
	    """,
    	[w_id]
    )
    for district in districts:
    	last_unfulfilled_order = district.last_unfulfilled_order
    	next_order_id = district.d_next_o_id
    	d_id = district.d_id
    	if last_unfulfilled_order == next_order_id:
    		continue
    	# a)
    	session.execute(
		    """
		    UPDATE  {keyspace}.district
		    	SET last_unfulfilled_order = %s
		    	WHERE w_id = %s 
		    	AND d_id = %s;
		    """,
	    	(last_unfulfilled_order + 1, w_id, d_id)
	    )
	    # b)
    	session.execute(
		    """
		    UPDATE  {keyspace}.order
		    	SET o_carrier_id = %s
		    	WHERE w_id = %s 
		    	AND d_id = %s;
		    """,
	    	(last_unfulfilled_order + 1, w_id, d_id)
	    )


