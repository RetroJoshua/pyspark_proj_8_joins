import init_context


def load_data():
    sc,sp = init_context.init_context()
    flight_2015 = sp.read.format("csv").option("header","true").option("mode","FAILFAST").option("inferSchema","True").load("Data/2015-summary.csv")
    flight_2014 = sp.read.format("JSON").option("header","true").option("mode","FAILFAST").option("inferSchema","True").load("Data/2014-summary.json")
    flight_2013 = sp.read.format("csv").option("header", "true").option("mode", "FAILFAST").option("inferSchema","True").load("Data/2013-summary.csv")
    return flight_2013,flight_2014, flight_2015




