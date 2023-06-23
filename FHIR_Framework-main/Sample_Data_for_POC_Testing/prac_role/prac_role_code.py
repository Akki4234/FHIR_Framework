from os import truncate
import sys

from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as pt
from pyspark.sql.window import Window as w
from awsglue.context import GlueContext
from datetime import datetime, timedelta,date
from awsglue.utils import getResolvedOptions
from pyspark.sql import types as pt
from pyspark.sql.functions import explode, explode_outer, col, when, split, element_at, to_date, lit, desc, dense_rank,concat_ws, collect_list, collect_set, udf,monotonically_increasing_id, length, spark_partition_id, row_number
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import Row
from functools import reduce
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel
import boto3

try:
    args = getResolvedOptions(sys.argv, ['last_date','last_hour'])
    last_date = args['last_date']
    last_hour = args['last_hour']

except:
    print('last_date argument is missing')
    last_date = None
    last_hour = None

try:
    args = getResolvedOptions(sys.argv, ['s3_input_path','s3_output_path', 'metadata_path', 'environment_path','JOB_NAME', 'environment', 'sns_arn'])
    environment_path = args['environment_path']
    s3_input_path = args['s3_input_path']
    job_name = args['JOB_NAME']
    environment = args['environment']
    sns_arn = args['sns_arn']
    job_run_id = args['JOB_RUN_ID']
    
    sns_client = boto3.client('sns')

    def union_all(*dfs):
        return reduce(DataFrame.union, dfs)

    def has_column(df, col):
        try:
            df[col]
            return True
        except AnalysisException:
            return False

    def col_check(df, col_name: str, col_rename: str):

        if has_column(df, col_name):
            final_df = df.withColumn(col_rename, col(col_name))
        else:
            # Adjust types according to your needs
            final_df = df.withColumn(col_rename, lit(None).cast("string"))
        return final_df

    @udf(returnType = pt.StringType())
    def format_col(data):
        return "~".join(s for s in data ).split("?")

    def agg_col_val_format(df,col_name,col_key, join_df):
        
        """[summary]
        """

        df = df.groupBy(col_key, 'versionId').agg(collect_set(col_name).alias(col_name))
        #df = df.groupBy(col_key).agg(F.first(col_name).alias(col_name))

        df = df.withColumn(col_name,format_col(df[col_name]))
        df = df.withColumn(col_name, when((length(col(col_name)) > 2), col(col_name)).otherwise(lit(None)))
        if df.rdd.isEmpty():
            join_df = join_df.drop(col_name).join(df, [col_key, 'versionId'], 'leftouter')
        else:
            join_df = join_df.drop(col_name).join(df, [col_key, 'versionId'], 'outer')
        return join_df

    def get_last_date(spark):
        try:
            practitioner_role_data = args["metadata_path"]
            # practitioner_role_data = 's3://bpd-datalake-qape-glue-metadata/PractitionerRole/'
            metadata_df = spark.read.option("header", True).csv(practitioner_role_data)
            metadata_df = metadata_df.where(metadata_df.job_name_met == 'practitioner_role')
            last_date = metadata_df.select('last_date').agg({'last_date': 'max'}).collect()[0][0]
            last_hour = metadata_df.select('last_hour').where(metadata_df.last_date==last_date).agg({'last_hour': 'max'}).collect()[0][0]

        except Exception as e:
            msg = 'Error reading metadata file or Metadata file has no data, check the metadata file'
            raise Exception(msg).with_traceback(e.__traceback__)
        return (last_date,last_hour)


    # based on initial_update column logic needs to modify
    def current_run_date(current_pdate,current_hour):
        try:
            sys_date=datetime.today()
            sys_hour = str(str(sys_date)[11:13])
            print('System Date-',sys_date)
            print('System hour-',sys_hour)
            if current_pdate >= sys_date:
                raise Exception(f'The current_date -{current_pdate} value is greater than or equal to system date hence Exiting....' )
        except Exception as e: 
                raise e

    def run_query(query, output_location):
        athena_client = boto3.client("athena")
        print(query)
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": output_location}
        )

        return response["QueryExecutionId"]

    def process_data(spark, last_date,last_hour):
        """[summary]

        :param spark: [description]
        :type spark: [type]
        """
        if last_date is not None:
            last_date = last_date
            last_hour = last_hour
            print(f'last_date and last_hour from workflow trigger-- {last_date} ,{last_hour}')
        else:
            last_date,last_hour = get_last_date(spark)
        print(f"last_pdate data has processed - {last_date}")
        schema = StructType([StructField('availableTime', ArrayType(StructType([StructField('availableEndTime', StringType(), True), StructField('availableStartTime', StringType(), True), StructField('daysOfWeek', ArrayType(StringType(), True), True)]), True), True), StructField('code', ArrayType(StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), True), StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueBoolean', BooleanType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('valueIdentifier', StructType([StructField('value', StringType(), True)]), True), StructField('valuePeriod', StructType([StructField('start', StringType(), True)]), True), StructField('valueReference', StructType([StructField('reference', StringType(), True)]), True)]), True), True), StructField('url', StringType(), True), StructField('valueBoolean', BooleanType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('valueReference', StructType([StructField('reference', StringType(), True)]), True)]), True), True), StructField('healthcareService', ArrayType(StructType([StructField('reference', StringType(), True)]), True), True), StructField('id', StringType(), True), StructField('identifier', ArrayType(StructType([StructField('type', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('value', StringType(), True)]), True), True), StructField('location', ArrayType(StructType([StructField('reference', StringType(), True)]), True), True), StructField('meta', StructType([StructField('lastUpdated', StringType(), True), StructField('profile', ArrayType(StringType(), True), True), StructField('source', StringType(), True), StructField('versionId', StringType(), True)]), True), StructField('organization', StructType([StructField('reference', StringType(), True)]), True), StructField('period', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), StructField('practitioner', StructType([StructField('reference', StringType(), True)]), True), StructField('resourceType', StringType(), True), StructField('active', StringType(), True), StructField('specialty', ArrayType(StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), True), StructField('text', StructType([StructField('div', StringType(), True), StructField('status', StringType(), True)]), True)])

        if isinstance(last_date, str):
            last_pdate = datetime.strptime(last_date, "%Y-%m-%d")
            print('Extracting the last_hour')
            last_hour = datetime.strptime(last_hour,'%H')
            current_hour =(last_hour  + timedelta(hours=1))
            current_hour = datetime.strftime(current_hour,'%H')
            print('current_hour',current_hour)
            if current_hour =='00':
                current_pdate = last_pdate + timedelta(1)
            else:
                current_pdate = last_pdate
            print(current_pdate,current_hour)
            year= current_pdate.strftime("%Y")
            month = current_pdate.strftime("%m")
            day= current_pdate.strftime("%d")
            current_run_date(current_pdate,current_hour)
            file_path = f"{s3_input_path}year={year}/month={month}/day={day}/hour={current_hour}/"
            
            
            try:
                print(f"Reading data from partition - {file_path}")
                
                df = spark.read.schema(schema).json(file_path, multiLine=True) \
                                .withColumn('year', F.lit(year)).withColumn('month', F.lit(month)).withColumn('day', F.lit(day)).withColumn('hour',F.lit(current_hour))
                
                if len(df.head(1)) > 0:
                    data_avail_flag = True
                else:
                    print(f'path - {file_path} exists but no data')
                    data_avail_flag = False
            except:
                data_avail_flag = False
                print(f"File path - {file_path} doesn't exist.")
        
        if data_avail_flag:
            df=df.select("resourceType","meta", "practitioner","organization", "id", "active", "location", "healthcareService", "extension", "specialty","code", "period", "identifier", "year", "month", "day","hour")

            extension_df = df.select("id","resourceType","meta", "active", "extension", 'organization', "year", "month", "day","hour") \
                                .withColumn("resourceType", df.resourceType) \
                                .withColumn("id", df.id) \
                                .withColumn("versionId", df.meta.versionId) \
                                .withColumn('PRACTITIONER_ROLE_LAST_UPDATE_DATE',df.meta.lastUpdated ) \
                                .drop(df.meta) \
                                .withColumn("extension_explode", explode_outer(df.extension)) \
                                .drop(df.extension) \
                                .withColumn("extension_extension_explode", explode_outer(col("extension_explode.extension"))) \
                                .withColumn("extension_extension_url", col("extension_extension_explode.url")) \
                                .withColumn("extension_extension_value_boolean", col("extension_extension_explode.valueBoolean")) \
                                .withColumn("extension_extension_value_codeable_concept_coding_explode", explode_outer(col("extension_extension_explode.valueCodeableConcept.coding"))) \
                                .withColumn("extension_extension_value_codeable_concept_coding_code", col("extension_extension_value_codeable_concept_coding_explode.code")) \
                                .withColumn("extension_extension_value_codeable_concept_coding_display", col("extension_extension_value_codeable_concept_coding_explode.display")) \
                                .drop(col("extension_extension_value_codeable_concept_coding_explode")) \
                                .withColumn("extension_extension_value_identifier_value", col("extension_extension_explode.valueIdentifier.value")) \
                                .withColumn("extension_extension_value_reference_reference", col("extension_extension_explode.valueReference.reference")) \
                                .drop(col("extension_extension_explode")) \
                                .withColumn("extension_url", col("extension_explode.url")) \
                                .withColumn("extension_value_boolean", col("extension_explode.valueBoolean")) \
                                .withColumn("extension_value_codeable_concept_coding_explode", explode_outer(col("extension_explode.valueCodeableConcept.coding"))) \
                                .withColumn("extension_value_codeable_concept_coding_code", col("extension_value_codeable_concept_coding_explode.code")) \
                                .withColumn("extension_value_codeable_concept_coding_system", col("extension_value_codeable_concept_coding_explode.system")) \
                                .drop("extension_value_codeable_concept_coding_explode") \
                                .withColumn("extension_value_reference_refernce", col("extension_explode.valueReference.reference")) \
                                .drop(col("extension_explode")) \
                                .withColumn("organization_explode", df.organization) \
                                .withColumn("organization_reference", col("organization_explode.reference")) \
                                .drop(col("organization_explode")) \
                                .drop(df.organization) \
                                .withColumn("ACTV_FLAG", df.active)
            
            print('exploded extension key')
            exten_extract_df = extension_df.withColumn('DISPLAY_INDICATOR', when((extension_df['extension_extension_url'] == "displayIndicatorProvider"), (extension_df['extension_extension_value_boolean']))) \
                    .withColumn('PCP_INDICATOR_CODE', when((extension_df['extension_extension_url'] == "pcpIndicatorCode"), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                    .withColumn('PCP_SELECTABILITY_CODE', when((extension_df['extension_extension_url'] == "pcpSelectabilityCode"), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                    .withColumn('ACCEPT_NEW_PATIENTS', when((extension_df['extension_extension_url'] == "acceptingPatients"), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                    .withColumn('PROVIDER_TERMINATION_CODE', when((extension_df['extension_value_codeable_concept_coding_system'] == 'http://bpd.bcbs.com/CodeSystem/BCBSProviderTerminationReasonCS'), (extension_df['extension_value_codeable_concept_coding_code']))) \
                    .withColumn('MEMB_SELF_ATRBTN_ACPTNC_IND', when((extension_df['extension_extension_url'] == 'providerAcceptsMemberSelectAttributionIndicator'), (extension_df['extension_extension_value_boolean']))) \
                    .withColumn('PROVIDER_CONTRACT_STATUS', when((extension_df['extension_url'] == 'http://bpd.bcbs.com/StructureDefinition/provcontract'), (extension_df['extension_value_boolean']))) \
                    .withColumn('network_id', when((extension_df['extension_url'] == 'http://bpd.bcbs.com/StructureDefinition/networkreference'), split(extension_df['extension_value_reference_refernce'], '/').getItem(1))) \
                    .withColumn('licensee_id', when((extension_df['extension_url'] == 'http://bpd.bcbs.com/StructureDefinition/licenseereference'), split(extension_df['extension_value_reference_refernce'], '/').getItem(1))) \
                    .withColumn('PROV_TIER_DESIG', when((extension_df['extension_url'] == "http://bpd.bcbs.com/StructureDefinition/provtierdesig"), (extension_df['extension_value_codeable_concept_coding_code']))) \
                    .withColumn('VBP', when((extension_df['extension_extension_url'] == "valueBasedProgramIdentifier"), (extension_df['extension_extension_value_identifier_value']))) \
                    .drop('extension_url', 'extension_value_codeable_concept_coding_code','extension_value_codeable_concept_coding_system', 'extension_value_reference_refernce', 'extension_value_boolean', 'extension_extension_value_boolean', 'extension_extension_value_codeable_concept_coding_display')    
            
            aff_df = exten_extract_df.select('id', 'versionId', 'extension_extension_url', 'extension_extension_value_codeable_concept_coding_code', 'extension_extension_value_reference_reference').where(exten_extract_df.extension_extension_url.isin('provAffType', 'provAffRef')) #.show(50, truncate=False)
            
            aff_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_role_aff_df_hourly/')
            aff_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_role_aff_df_hourly/')

            print('processed aff_df')

            tmp_df = aff_df.select('id','versionId', 'extension_extension_value_codeable_concept_coding_code').where(aff_df.extension_extension_value_codeable_concept_coding_code.isin("ADMITPRIV", "FACILITY", "HCSYSTEM"))
            tmp_df = tmp_df.where(tmp_df.extension_extension_value_codeable_concept_coding_code.isNotNull()).withColumnRenamed('extension_extension_value_codeable_concept_coding_code', 'ref_code').withColumn('seq_col',F.monotonically_increasing_id())
            tmp_df1 = aff_df.select('id','versionId', 'extension_extension_value_reference_reference').where(aff_df.extension_extension_value_reference_reference.isNotNull()).withColumnRenamed('extension_extension_value_reference_reference', 'ref_id')
            tmp_df1 = tmp_df1.withColumn('seq_col',F.monotonically_increasing_id())
            jn_df = tmp_df.join(tmp_df1, ['seq_col', 'versionId', 'id'], 'inner').drop('seq_col').distinct()

            ref_df = exten_extract_df.drop('extension_extension_url', 'extension_extension_value_identifier_value','extension_extension_value_reference_reference').join(jn_df, ['id', 'versionId'], 'left').distinct()

            ref_df = ref_df.withColumn("ADMIT_PRIV_ref_id", when((F.col('ref_code') == "ADMITPRIV"),F.split(F.col('ref_id'), '/').getItem(1)).otherwise(lit(None))) \
                                .withColumn("FCLTY_AFFLT_ref_id", when((F.col('ref_code') == "FACILITY"),F.split(F.col('ref_id'), '/').getItem(1)).otherwise(lit(None))) \
                                .withColumn("HCSYSTEM_ref_id", when((F.col('ref_code') == "HCSYSTEM"),F.split(F.col('ref_id'), '/').getItem(1)).otherwise(lit(None))) \
                                .withColumn("PROVGRP_ref_id", when((ref_df.extension_extension_value_codeable_concept_coding_code.isin("PROVGRP")),split(ref_df['organization_reference'], '/').getItem(1)).otherwise(lit(None))) \
                                .drop('extension_extension_value_codeable_concept_coding_code', 'organization_reference', 'ref_code', 'ref_id') \
                                .distinct()


            print('processed ref_df')

            vbp = ref_df.select('id', 'versionId', 'VBP').dropDuplicates()
            extension_df = agg_col_val_format(df=vbp,col_name='VBP',col_key='id', join_df=ref_df).withColumn('seq_col',row_number().over(w.partitionBy("id").orderBy('id')))

            print('processed extension key')
            ### REFERENCE DATA

            reference_df = df.select("id", "practitioner", "healthcareService", "location", "meta", "year", "month", "day","hour") \
                            .withColumn("versionId", df.meta.versionId) \
                            .drop(df.meta) \
                            .withColumn("practitioner_explode", df.practitioner) \
                            .withColumn("practitioner_reference", col("practitioner_explode.reference")) \
                            .drop(col("practitioner_explode")) \
                            .drop(df.practitioner) \
                            .withColumn("healthcare_service_explode", explode_outer(df.healthcareService)) \
                            .withColumn("healthcare_service_reference", col("healthcare_service_explode.reference")) \
                            .drop(col("healthcare_service_explode")) \
                            .drop(df.healthcareService) \
                            .withColumn("location_explode", explode_outer(df.location)) \
                            .drop(df.location) \
                            .withColumn("location_reference", col("location_explode.reference")) \
                            .drop(col("location_explode")) \
                            .withColumn('seq_col',row_number().over(w.partitionBy("id").orderBy('id'))) \
            
            #### CODE
            code_extr_df = df.select("id","period","code", "meta", "year", "month", "day","hour") \
                        .withColumn("versionId", df.meta.versionId) \
                        .drop(df.meta) \
                        .withColumn("PROVIDER_NETWORK_START_DATE", df.period.start.cast("string")) \
                        .withColumn("PROVIDER_NETWORK_END_DATE", df.period.end.cast("string")) \
                        .drop(df.period) \
                        .withColumn("code_explode", explode_outer(df.code)) \
                        .drop(df.code) \
                        .withColumn("code_coding_explode", explode_outer(col("code_explode.coding"))) \
                        .withColumn("code_coding_code", col("code_coding_explode.code")) \
                        .withColumn("code_coding_system", col("code_coding_explode.system")) \
                        .withColumn("code_coding_display", col("code_coding_explode.display")) \
                        .drop('code_explode', 'code_coding_explode') \


            code_df = code_extr_df.withColumn('ENTITY_TYPE_CODE', when((code_extr_df['code_coding_system'] == 'http://bpd.bcbs.com/CodeSystem/BCBSProviderEntityTypeCS'), (code_extr_df['code_coding_code']))) \
                                        .withColumn('PROVIDER_TYPE_CODE', when((code_extr_df['code_coding_system'] == 'http://bpd.bcbs.com/CodeSystem/BCBSProviderTypeCS'), (code_extr_df['code_coding_code']))) \
                                        .withColumn('PROV_TYPE_NAME', when((code_extr_df['code_coding_system'] == 'http://bpd.bcbs.com/CodeSystem/BCBSProviderTypeCS'), (code_extr_df['code_coding_display']))) \
                                        .drop('code_coding_code', 'code_coding_system', 'code_coding_display') \
                                        .withColumn('seq_col',row_number().over(w.partitionBy("id").orderBy('id'))) \

            specialty_extr_df = df.select("id", "specialty", "meta", "year", "month", "day","hour") \
                            .withColumn("versionId", df.meta.versionId) \
                            .drop(df.meta) \
                            .withColumn("specialty_explode", explode_outer(df.specialty)) \
                            .withColumn("specialty_coding_explode", explode_outer(col("specialty_explode.coding"))) \
                            .withColumn("specialty_coding_code", col("specialty_coding_explode.code")) \
                            .withColumn("specialty_coding_system", col("specialty_coding_explode.system")) \
                            .drop(df.specialty) \
                            .drop(col("specialty_coding_explode")) \
                            .drop(col("specialty_explode")) \
        

            specialty_extr_df = specialty_extr_df.withColumn('PROVIDER_TAXONOMY_CODE', when((specialty_extr_df['specialty_coding_system'] == "http://nucc.org/provider-taxonomy"), (specialty_extr_df['specialty_coding_code']))) \
                                                .drop('specialty_coding_code', 'specialty_coding_system')

            specialty_extr_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_role_specialty_extr_df_hourly/')
            specialty_extr_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_role_specialty_extr_df_hourly/')

            print('processed specialty_extr_df')

            tax_df =  specialty_extr_df.select('id', 'versionId', 'PROVIDER_TAXONOMY_CODE').dropDuplicates()   
            specialty_df = agg_col_val_format(df=tax_df,col_name='PROVIDER_TAXONOMY_CODE',col_key='id', join_df=specialty_extr_df).withColumn('seq_col',row_number().over(w.partitionBy("id").orderBy('id'))) 


            identifier_extr_df = df.select("id", "identifier", "meta", "year", "month", "day","hour") \
                            .withColumn("versionId", df.meta.versionId) \
                            .drop(df.meta) \
                            .withColumn("identifier_explode", explode_outer(df.identifier)) \
                            .withColumn("identifier_explode_value", col("identifier_explode.value")) \
                            .withColumn("identifier_type_coding_explode", explode_outer(col("identifier_explode.type.coding"))) \
                            .withColumn("identifier_type_coding_code", col("identifier_type_coding_explode.code")) \
                            .drop(col("identifier_type_coding_explode")) \
                            .drop(col("identifier_explode")) \
                            .drop(df.identifier) \
            
            identifier_extr_df = identifier_extr_df.withColumn('PROVIDER_ENTITY_IDENTIFIER', when((identifier_extr_df['identifier_type_coding_code'] == 'PEI'), (identifier_extr_df['identifier_explode_value']))) \
                                                    .drop('identifier_type_coding_code', 'identifier_explode_value')

            identifier_extr_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_role_identifier_extr_df_hourly/')
            identifier_extr_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_role_identifier_extr_df_hourly/')

            print('processed identifier_extr_df')

            pei_df =  identifier_extr_df.select('id', 'versionId', 'PROVIDER_ENTITY_IDENTIFIER').dropDuplicates() 
            identifier_df = agg_col_val_format(df=pei_df,col_name='PROVIDER_ENTITY_IDENTIFIER',col_key='id', join_df=identifier_extr_df).withColumn('seq_col',row_number().over(w.partitionBy("id").orderBy('id')))  

            join_key = ['id','seq_col', 'versionId','year', 'month', 'day',"hour"]
            ext_ref_df = extension_df.join(reference_df, join_key, 'outer').dropDuplicates()
            ext_ref_code_df = ext_ref_df.join(code_df, join_key, 'outer').dropDuplicates()
            ext_ref_code_spec_df = ext_ref_code_df.join(specialty_df, join_key, 'outer').dropDuplicates()
            join_df = ext_ref_code_spec_df.join(identifier_df, join_key, 'outer').drop('seq_col').dropDuplicates()

            print('Writing join_df')
            join_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_role_join_df_hourly/')
            join_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_role_join_df_hourly/')

            print('processed join_df')
            

            col_list = join_df.columns 
            for each_col in col_list:
                if each_col in ['id', 'versionId','year', 'month', 'day',"hour"]:
                    pass
                else:
                    join_df = join_df.withColumn('flag', F.when(F.col(each_col).isNull(),F.lit(0)).otherwise(F.lit(1)))
                    join_df = join_df.withColumn(each_col , F.last(each_col,True).over(w.partitionBy('id', 'versionId','year', 'month', 'day','hour').orderBy(F.col('flag').desc()).rowsBetween(-sys.maxsize,0))).drop('flag')  
                    join_df = join_df.distinct()

            df_output = join_df

            df_output.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_role_df_output_hourly/')
            df_output = spark.read.parquet(f's3://{environment_path}/tmp/prac_role_df_output_hourly/')

            print('joining network and location')

            network_df = df_output.select('id', 'versionId', 'network_id').distinct()
            location_df = df_output.select('id', 'versionId', 'location_reference').distinct()

            net_location_df = location_df.join(network_df, ['id', 'versionId'], 'outer')
            df_final = df_output.drop('location_reference', 'network_id')
            df_final = net_location_df.join(df_final, ['id', 'versionId'], 'outer').distinct()
            
            print('joined network and location')

            print('***************Storing data in target location ***************')
            df_output = df_final.withColumn('month', F.when(F.length(F.col('month')) == 1, F.concat(F.lit('0'), F.col('month'))).otherwise(F.col('month'))) \
                                .withColumn('day', F.when(F.length(F.col('day')) == 1, F.concat(F.lit('0'), F.col('day'))).otherwise(F.col('day')))\
                                .withColumn('hour', F.when(F.length(F.col('hour')) == 1, F.concat(F.lit('0'), F.col('hour'))).otherwise(F.col('hour')))
            
            output_bucket = args["s3_output_path"]
            df_output.write.mode('overwrite').format('parquet').partitionBy('year', 'month', 'day','hour').option("partitionOverwriteMode", "dynamic").save(output_bucket)
            print('PractitionerRole Data loaded')

            envName = args['environment'].lower() 
            # Adding raw-zone table partitions

            raw_table = f'bpd-datalake-{envName}-rawzone-hourly-db.rawzone_practitionerrole_hourly'
            raw_query = f"alter table `{raw_table}` ADD IF NOT EXISTS PARTITION(year={year}, month={month}, day={day}, hour={current_hour}) location '{s3_input_path}year={year}/month={month}/day={day}/hour={current_hour}'"
            run_query(query=raw_query, output_location=f's3://bpd-datalake-{envName}-zone/athena/')
            print(f'added partitions for {raw_table}')

            # Adding flatten zone table partitions

            table_name = f'bpd-datalake-{envName}-flattenzone-hourly-db.flattenzone_practitionerrole_hourly'
            query = f"msck repair table `{table_name}`"
            run_query(query=query, output_location=f's3://bpd-datalake-{envName}-zone/athena/')
            print(f'added partitions for {table_name}')

        else:
            print(f'Practitioner_role data not available at source - {current_pdate} for hour -{current_hour }')
        print('Checking the current_pdate')
        
        return current_pdate ,current_hour
        
        

    if __name__ == "__main__":
        run_date = datetime.today()
        current_pdate = None
        glueContext = GlueContext(SparkContext.getOrCreate())
        spark = glueContext.spark_session
        current_pdate,current_hour = process_data(spark, last_date,last_hour)

        current_pdate = datetime.strptime((current_pdate).strftime('%Y-%m-%d'),"%Y-%m-%d").date()

        # Prepare metadata table
        print("Initiating processing for metadata table")
        processing_time = datetime.now()
        job_name_met = 'practitioner_role'
        print(f"last+p_date {current_pdate} ----------- processing_time {processing_time} ---------")
        meta_data_path = args["metadata_path"]
        meta_data_df = spark.createDataFrame([(current_pdate, processing_time, job_name_met,current_hour)],
                                    ['last_date', 'run_date', 'job_name_met','last_hour'])
        meta_data_df.coalesce(1).write.mode('append').format('csv').option('header', 'true').save(meta_data_path)
        print("Processed metadata table")

        
except Exception as e:
    # preparing data for SNS notification
    status = 'FINISHED_FAILURE'
    if current_pdate: 
        message = (f' Job_Name: {job_name}\n Status: {status} \n environment: {environment} \n Job_Run_ID: {job_run_id} \n Data_Date: {current_pdate} \
        \n Data_Hour: {current_hour} \n Run_Date: {run_date} \n ERROR: {e}')
    else: 
        message = (f' Job_Name: {job_name}\n Status: {status} \n environment: {environment} \n Job_Run_ID: {job_run_id} \n Run_Date: {run_date} \n Data_Hour: {current_hour} ERROR: {e}')
    subject = (f'[{environment}] {job_name} {status}')
    sns_client.publish(TopicArn=sns_arn,
        Message=message,
        Subject=subject)
    raise e
