import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1734998657953 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1734998657953")

# Script generated for node Customer Trusted
CustomerTrusted_node1734998674455 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734998674455")

# Script generated for node Join
CustomerTrusted_node1734998674455DF = CustomerTrusted_node1734998674455.toDF()
AccelerometerTrusted_node1734998657953DF = AccelerometerTrusted_node1734998657953.toDF()
Join_node1734998712290 = DynamicFrame.fromDF(CustomerTrusted_node1734998674455DF.join(AccelerometerTrusted_node1734998657953DF, (CustomerTrusted_node1734998674455DF['email'] == AccelerometerTrusted_node1734998657953DF['user']), "leftsemi"), glueContext, "Join_node1734998712290")

# Script generated for node Customer Curated
CustomerCurated_node1735000027381 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1734998712290, database="stedi", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="CustomerCurated_node1735000027381")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1734998712290, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734996326396", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734999020714 = glueContext.getSink(path="s3://project-bucket-udacity-mar/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734999020714")
AmazonS3_node1734999020714.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1734999020714.setFormat("json")
AmazonS3_node1734999020714.writeFrame(Join_node1734998712290)
job.commit()