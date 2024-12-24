import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1734990802193 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AmazonS3_node1734990802193")

# Script generated for node SQL Query
SqlQuery1822 = '''
select * from myDataSource
where myDataSource.shareWithResearchAsOfDate is not null
-- where myDataSource.customerName != "Santosh Clayton"
'''
SQLQuery_node1734991468332 = sparkSqlQuery(glueContext, query = SqlQuery1822, mapping = {"myDataSource":AmazonS3_node1734990802193}, transformation_ctx = "SQLQuery_node1734991468332")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1734994528253 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1734991468332, database="stedi", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1734994528253")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734991468332, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734992297812", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734992735313 = glueContext.getSink(path="s3://project-bucket-udacity-mar/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734992735313")
AmazonS3_node1734992735313.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1734992735313.setFormat("json")
AmazonS3_node1734992735313.writeFrame(SQLQuery_node1734991468332)
job.commit()