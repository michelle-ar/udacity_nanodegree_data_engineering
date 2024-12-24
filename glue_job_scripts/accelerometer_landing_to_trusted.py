import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1734995406076 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734995406076")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1734993079640 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1734993079640")

# Script generated for node Join
AccelerometerLanding_node1734993079640DF = AccelerometerLanding_node1734993079640.toDF()
CustomerTrusted_node1734995406076DF = CustomerTrusted_node1734995406076.toDF()
Join_node1734995422362 = DynamicFrame.fromDF(AccelerometerLanding_node1734993079640DF.join(CustomerTrusted_node1734995406076DF, (AccelerometerLanding_node1734993079640DF['user'] == CustomerTrusted_node1734995406076DF['email']), "leftsemi"), glueContext, "Join_node1734995422362")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1735006659903 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1734995422362, database="stedi", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1735006659903")

job.commit()