from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

chicago_crimes_schema = StructType([
    StructField("id", StringType(), True),
    StructField("case_number", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("block", StringType(), True),
    StructField("iucr", StringType(), True),
    StructField("primary_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("arrest", BooleanType(), True),
    StructField("domestic", BooleanType(), True),
    StructField("beat", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("community_area", StringType(), True),
    StructField("fbi_code", StringType(), True),
    StructField("x_coordinate", StringType(), True),
    StructField("y_coordinate", StringType(), True),
    StructField("year", StringType(), True),
    StructField("updated_on", TimestampType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("human_address", StringType(), True)
    ]), True)
])

seattle_crimes_schema = StructType([
    StructField("report_number", StringType(), True),  
    StructField("report_date_time", TimestampType(), True),  
    StructField("offense_id", StringType(), True),  
    StructField("offense_date", TimestampType(), True),  
    StructField("nibrs_group_a_b", StringType(), True),  
    StructField("nibrs_crime_against_category", StringType(), True),  
    StructField("offense_sub_category", StringType(), True),
    StructField("shooting_type_group", StringType(), True),  
    StructField("block_address", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("beat", StringType(), True),
    StructField("precinct", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("reporting_area", StringType(), True),
    StructField("offense_category", StringType(), True),
    StructField("nibrs_offense_code_description", StringType(), True),
    StructField("nibrs_offense_code", StringType(), True)
])