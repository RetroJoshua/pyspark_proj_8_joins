# SPARK JOINS

This project is to perform `inner`, all outer joins and semi joins.

## `create_df.py`:
 - `load_data.py` : helps to put data into Spark data frames.

## `data_man.py`: 
- `left_semi_join()`: Semi joins are a bit of a departure from the other joins. They do not actually include any values from
    the right DataFrame. They only compare values to see if the value exists in the second DataFrame. If
    the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left
    DataFrame. Think of left semi joins as filters on a DataFrame, as opposed to the function of a
    conventional join.

- `left_anti_join()`:  Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually include any
    values from the right DataFrame. They only compare values to see if the value exists in the second
    DataFrame.
- `right_outer_join()`: Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame.
    If there is no equivalent row in the left DataFrame, Spark will insert null:
- `outer_join():`: Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the
    rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame,
    Spark will insert null:
- `left_outer_join()`: Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If
    there is no equivalent row in the right DataFrame, Spark will insert null:
- `inner_join()`: Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together) only
    the rows that evaluate to true.
- `outer_join()`: Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the
    rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame,
    Spark will insert null.
- `cross_join()`: The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
    joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame to
    ever single row in the right DataFrame. This will cause an absolute explosion in the number of rows
    contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the cross-join of
    these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very explicitly state
    that you want a cross-join by using the cross join keyword.

## `Data` Folder:
- Contains flight data `2015-summary.csv`, `2014-summary.json` and `2013-summary.csv`. 


## `main.py`:
- has to implement.