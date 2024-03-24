import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Netflix_Data_Catalog_source
Netflix_Data_Catalog_source_node1711180192390 = glueContext.create_dynamic_frame.from_catalog(database="netflix", table_name="input", transformation_ctx="Netflix_Data_Catalog_source_node1711180192390")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1711180438089_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        ColumnValues "imdb_score" >= 7.0
    ]
"""

EvaluateDataQuality_node1711180438089 = EvaluateDataQuality().process_rows(frame=Netflix_Data_Catalog_source_node1711180192390, ruleset=EvaluateDataQuality_node1711180438089_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1711180438089", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1711180709553 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1711180438089, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1711180709553")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1711180713431 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1711180438089, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1711180713431")

# Script generated for node Conditional Router
ConditionalRouter_node1711182808762 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1711180713431,
  group_filters = [GroupFilter(name = "Failed_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node Failed_records
Failed_records_node1711182809899 = SelectFromCollection.apply(dfc=ConditionalRouter_node1711182808762, key="Failed_records", transformation_ctx="Failed_records_node1711182809899")

# Script generated for node default_group
default_group_node1711182809742 = SelectFromCollection.apply(dfc=ConditionalRouter_node1711182808762, key="default_group", transformation_ctx="default_group_node1711182809742")

# Script generated for node Drop_columns_and_change_dataType
Drop_columns_and_change_dataType_node1711184078843 = ApplyMapping.apply(frame=default_group_node1711182809742, mappings=[("index", "long", "index", "long"), ("id", "string", "id", "string"), ("title", "string", "title", "string"), ("type", "string", "type", "string"), ("description", "string", "description", "string"), ("release_year", "long", "release_year", "long"), ("age_certification", "string", "age_certification", "string"), ("runtime", "long", "runtime", "long"), ("imdb_id", "string", "imdb_id", "string"), ("imdb_score", "double", "imdb_score", "double"), ("imdb_votes", "double", "imdb_votes", "double")], transformation_ctx="Drop_columns_and_change_dataType_node1711184078843")

# Script generated for node Data_quality_rule_outcome_dump
Data_quality_rule_outcome_dump_node1711180804049 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1711180709553, connection_type="s3", format="json", connection_options={"path": "s3://netflix-movie-show-data/outputs/rule_outcome/", "partitionKeys": []}, transformation_ctx="Data_quality_rule_outcome_dump_node1711180804049")

# Script generated for node Bad_quality_data_dump
Bad_quality_data_dump_node1711183882841 = glueContext.write_dynamic_frame.from_options(frame=Failed_records_node1711182809899, connection_type="s3", format="json", connection_options={"path": "s3://netflix-movie-show-data/outputs/bad_quality_data_dump/", "partitionKeys": []}, transformation_ctx="Bad_quality_data_dump_node1711183882841")

# Script generated for node Redshift_load
Redshift_load_node1711184479310 = glueContext.write_dynamic_frame.from_catalog(frame=Drop_columns_and_change_dataType_node1711184078843, database="netflix", table_name="dev_netflix_netflix_destination_table", redshift_tmp_dir="s3://netflix-movie-show-data/outputs/temp_redshift_load/",additional_options={"aws_iam_role": "arn:aws:iam::381491980262:role/redhshiftRole"}, transformation_ctx="Redshift_load_node1711184479310")

job.commit()