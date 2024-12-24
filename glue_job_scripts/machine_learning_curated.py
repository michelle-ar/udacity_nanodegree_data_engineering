import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Accelerator trusted
Acceleratortrusted_node1735001314799 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Acceleratortrusted_node1735001314799")

# Script generated for node Step Trusted
StepTrusted_node1735001314259 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trusted", transformation_ctx="StepTrusted_node1735001314259")

# Script generated for node SQL Query
SqlQuery1780 = '''
select * from step
JOIN acc on acc.timestamp = step.sensorreadingtime
-- and acc.user = step.email
'''
SQLQuery_node1735004491337 = sparkSqlQuery(glueContext, query = SqlQuery1780, mapping = {"step":StepTrusted_node1735001314259, "acc":Acceleratortrusted_node1735001314799}, transformation_ctx = "SQLQuery_node1735004491337")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1735006903456 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1735004491337, database="stedi", table_name="machine_learning", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1735006903456")

job.commit()