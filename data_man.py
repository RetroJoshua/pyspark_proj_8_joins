import create_df


def left_semi_join():
    # Semi joins are a bit of a departure from the other joins. They do not actually include any values from
    # the right DataFrame. They only compare values to see if the value exists in the second DataFrame. If
    # the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left
    # DataFrame. Think of left semi joins as filters on a DataFrame, as opposed to the function of a
    # conventional join:

    flight_2013,flight_2014,flight_2015 = create_df.load_data()
    joinType = "left_semi"
    join_expression = flight_2015["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014, join_expression, joinType)
    return result

def left_anti_join():
    #Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually include any
    # values from the right DataFrame. They only compare values to see if the value exists in the second
    # DataFrame.

    flight_2013,flight_2014,flight_2015 = create_df.load_data()
    joinType = "left_anti"
    join_expression = flight_2015["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014, join_expression, joinType)
    return result

def right_outer_join():
    #Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    #right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame.
    #If there is no equivalent row in the left DataFrame, Spark will insert null:

    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    joinType = "right_outer"
    join_expression = flight_2015["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014, join_expression, joinType)
    return result

def outer_join():
    #Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the
    #rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame,
    #Spark will insert null:

    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    joinType = "outer"
    join_expression = flight_2015["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014, join_expression, joinType)
    return result


def left_outer_join():
    # Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    # left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If
    # there is no equivalent row in the right DataFrame, Spark will insert null:


    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    joinType = "left_outer"
    join_expression = flight_2015["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014, join_expression, joinType)
    return result



def inner_join():
    # Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together) only
    # the rows that evaluate to true.

    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    join_expression = flight_2013["DEST_COUNTRY_NAME"]==flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2013.join(flight_2014,join_expression)
    return result



def outer_join():
    # Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the
    # rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame,
    # Spark will insert null:

    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    join_type = "outer"
    join_expression = flight_2014["DEST_COUNTRY_NAME"] == flight_2015["DEST_COUNTRY_NAME"]
    result=flight_2013.join(flight_2015,join_expression,join_type)
    return result


def cross_join():
    #The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
    #joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame to
    #ever single row in the right DataFrame. This will cause an absolute explosion in the number of rows
    #contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the cross-join of
    #these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very explicitly state
    #that you want a cross-join by using the cross join keyword:

    flight_2013, flight_2014, flight_2015 = create_df.load_data()
    joinType = "cross"
    join_expression = flight_2013["DEST_COUNTRY_NAME"] == flight_2014["DEST_COUNTRY_NAME"]
    result = flight_2015.join(flight_2014,join_expression, joinType)
    return result



