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

# Script generated for node Customer Curated
CustomerCurated_node1734999894585 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1734999894585")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1734999980041 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer", transformation_ctx="StepTrainerLanding_node1734999980041")

# Script generated for node Join
CustomerCurated_node1734999894585DF = CustomerCurated_node1734999894585.toDF()
StepTrainerLanding_node1734999980041DF = StepTrainerLanding_node1734999980041.toDF()
Join_node1735000618289 = DynamicFrame.fromDF(CustomerCurated_node1734999894585DF.join(StepTrainerLanding_node1734999980041DF, (CustomerCurated_node1734999894585DF['serialnumber'] == StepTrainerLanding_node1734999980041DF['serialnumber']), "left"), glueContext, "Join_node1735000618289")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1735006551819 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1735000618289, database="stedi", table_name="step_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1735006551819")

job.commit()