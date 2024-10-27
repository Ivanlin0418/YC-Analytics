from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

"""
This file contains the schemas for the data that we are loading into Spark.
"""

#------------SCHEMAS-------------
"""
badges ->          id,badge
companies ->       id,name,slug,website,smallLogoUrl,oneLiner,longDescription,teamSize,url,batch,status
founders ->        first_name,last_name,hnid,avatar_thumb,current_company,current_title,company_slug,top_company
industries ->      id,industry
prior_companies -> hnid,company
regions ->         id,region,country,address
schools ->         hnid,school,field_of_study,year
tags ->            id,tag
"""
#--------------------------------

BADGES_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("badge", StringType(), True),
])

COMPANY_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("website", StringType(), True),
    StructField("smallLogoUrl", StringType(), True),
    StructField("oneLiner", StringType(), True),
    StructField("longDescription", StringType(), True),
    StructField("teamSize", FloatType(), True),
    StructField("url", StringType(), True),
    StructField("batch", StringType(), True),
    StructField("status", StringType(), True),
])

FOUNDERS_SCHEMA = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("hnid", StringType(), True),
    StructField("avatar_thumb", StringType(), True),
    StructField("current_company", StringType(), True),
    StructField("current_title", StringType(), True),
    StructField("company_slug", StringType(), True),
    StructField("top_company", StringType(), True),
])

INDUSTRIES_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("industry", StringType(), True),
])

PRIOR_COMPANIES_SCHEMA = StructType([
    StructField("hnid", StringType(), True),
    StructField("company", StringType(), True),
])

REGIONS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("address", StringType(), True),
])

SCHOOLS_SCHEMA = StructType([
    StructField("hnid", StringType(), True),
    StructField("school", StringType(), True),
    StructField("field_of_study", StringType(), True),
    StructField("year", StringType(), True),
])

TAGS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("tag", StringType(), True),
])  



