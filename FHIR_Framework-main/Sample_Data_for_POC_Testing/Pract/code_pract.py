#pract hourly
from ntpath import join
from os import truncate
import sys

import pyspark
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as pt
from pyspark.sql.window import Window as w
from awsglue.context import GlueContext
from datetime import datetime, timedelta, date
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import explode, explode_outer, col, when, split, element_at, to_date, lit, desc, dense_rank,concat_ws, concat, collect_set, udf, row_number, length, initcap, coalesce
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import Row
from functools import reduce
from pyspark.sql.utils import AnalysisException
import logging
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
        return "~".join(s for s in data ).split("?^")

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
            practitioner_data = args["metadata_path"]
            # practitioner_role_data = 's3://bpd-datalake-qape-glue-metadata/PractitionerRole/'
            metadata_df = spark.read.option("header", True).csv(practitioner_data)
            metadata_df = metadata_df.where(metadata_df.job_name_met == 'practitioner')
            last_date = metadata_df.select('last_date').agg({'last_date': 'max'}).collect()[0][0]
            last_hour = metadata_df.select('last_hour').where(metadata_df.last_date==last_date).agg({'last_hour': 'max'}).collect()[0][0]

        except Exception as e:
            msg = 'Error reading metadata file or Metadata file has no data, check the metadata file'
            raise Exception(msg).with_traceback(e.__traceback__)
        return (last_date,last_hour)

    def union_all(*dfs):
        return reduce(DataFrame.union, dfs)

    def null_datatype_conversion(df):
        for each_col in df.columns:
            if df.select(each_col).dtypes[0][1] in ['void', 'null', 'struct']:
                col_name = df.select(each_col).dtypes[0][0]
                df = df.withColumn(col_name, F.col(col_name).cast(pt.StringType()))
        return df

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
        
        
        # schema = StructType([StructField('communication', ArrayType(StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), True), StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True)]), True), True)]), True), StructField('valuePeriod', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), StructField('valueReference', StructType([StructField('reference', StringType(), True)]), True)]), True), True), StructField('url', StringType(), True)]), True), True), StructField('gender', StringType(), True), StructField('id', StringType(), True), StructField('identifier', ArrayType(StructType([StructField('period', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), StructField('type', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('value', StringType(), True)]), True), True), StructField('meta', StructType([StructField('lastUpdated', StringType(), True), StructField('profile', ArrayType(StringType(), True), True), StructField('source', StringType(), True), StructField('versionId', StringType(), True)]), True), StructField('name', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueString', StringType(), True)]), True), True), StructField('family', StringType(), True), StructField('given', ArrayType(StringType(), True), True), StructField('prefix', ArrayType(StringType(), True), True), StructField('suffix', ArrayType(StringType(), True), True)]), True), True), StructField('qualification', ArrayType(StructType([StructField('code', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True), StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCode', StringType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True)]), True), True), StructField('url', StringType(), True), StructField('valueBoolean', BooleanType(), True)]), True), True)]), True), StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCode', StringType(), True)]), True), True), StructField('identifier', ArrayType(StructType([StructField('value', StringType(), True)]), True), True), StructField('issuer', StructType([StructField('reference', StringType(), True)]), True), StructField('period', StructType([StructField('end', StringType(), True)]), True)]), True), True), StructField('resourceType', StringType(), True), StructField('telecom', ArrayType(StructType([StructField('system', StringType(), True), StructField('use', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('text', StructType([StructField('div', StringType(), True), StructField('status', StringType(), True)]), True)])
        schema = StructType([StructField('communication', ArrayType(StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), True), StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True)]), True), True)]), True), StructField('valuePeriod', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), StructField('valueReference', StructType([StructField('reference', StringType(), True)]), True)]), True), True), StructField('url', StringType(), True)]), True), True), StructField('gender', StringType(), True), StructField('id', StringType(), True), StructField('identifier', ArrayType(StructType([StructField('period', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), StructField('system',StringType(),True), StructField('type', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('value', StringType(), True)]), True), True), StructField('meta', StructType([StructField('lastUpdated', StringType(), True), StructField('profile', ArrayType(StringType(), True), True), StructField('source', StringType(), True), StructField('versionId', StringType(), True)]), True), StructField('name', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueString', StringType(), True)]), True), True), StructField('family', StringType(), True), StructField('given', ArrayType(StringType(), True), True), StructField('prefix', ArrayType(StringType(), True), True), StructField('suffix', ArrayType(StringType(), True), True)]), True), True), StructField('qualification', ArrayType(StructType([StructField('code', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True), StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCode', StringType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True)]), True), True), StructField('url', StringType(), True), StructField('valueBoolean', BooleanType(), True)]), True), True)]), True), StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCode', StringType(), True)]), True), True), StructField('identifier', ArrayType(StructType([StructField('value', StringType(), True)]), True), True), StructField('issuer', StructType([StructField('reference', StringType(), True)]), True), StructField('period', StructType([StructField('end', StringType(), True)]), True)]), True), True), StructField('resourceType', StringType(), True), StructField('telecom', ArrayType(StructType([StructField('system', StringType(), True), StructField('use', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('text', StructType([StructField('div', StringType(), True), StructField('status', StringType(), True)]), True)])
        
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
                
                practitioner_df = spark.read.schema(schema).json(file_path, multiLine=True) \
                                .withColumn('year', F.lit(year)).withColumn('month', F.lit(month)).withColumn('day', F.lit(day)).withColumn('hour',F.lit(current_hour))
                
                if len(practitioner_df.head(1)) > 0:
                    data_avail_flag = True
                else:
                    print(f'path - {file_path} exists but no data')
                    data_avail_flag = False
            except:
                data_avail_flag = False
                print(f"File path - {file_path} doesn't exist.")

        
        if data_avail_flag:  
            df = practitioner_df.select("resourceType", "gender", "meta", "id", "extension",\
                                        "telecom", "identifier", "qualification", "communication", \
                                        "name","year", "month", "day","hour")

            extract_qual_df = df.select("resourceType", "gender", "meta", "id", "name","qualification", "year", "month", "day","hour") \
                                            .withColumn("id", practitioner_df.id) \
                                            .withColumn("resourceType", practitioner_df.resourceType) \
                                            .withColumn("versionId", practitioner_df.meta.versionId) \
                                            .withColumn('PRACTITIONER_LAST_UPDATE_DATE',practitioner_df.meta.lastUpdated ) \
                                            .drop(practitioner_df.meta) \
                                            .withColumn("GENDER", practitioner_df.gender) \
                                            .withColumn("name_explode", explode_outer(practitioner_df.name)) \
                                            .withColumn("LAST_NAME", col("name_explode.family")) \
                                            .withColumn("name_given", col("name_explode.given")) \
                                            .withColumn("NAME_SUFFIX", explode_outer(col("name_explode.suffix"))) \
                                            .withColumn("NAME_PREFIX", explode_outer(col("name_explode.prefix"))) \
                                            .withColumn("name_extension_explode", explode_outer(col("name_explode.extension"))) \
                                            .withColumn("PROVIDER_TITLE", col("name_extension_explode.valueString")) \
                                            .drop(col("name_explode")) \
                                            .drop(col("name_extension_explode")) \
                                            .drop(practitioner_df.name) \
                                            .withColumn("qualification_explode", explode_outer(practitioner_df.qualification)) \
                                            .drop(practitioner_df.qualification) \
                                            .withColumn("qualification_code_coding_explode", explode_outer(col("qualification_explode.code.coding"))) \
                                            .withColumn("qualificaition_code_coding_code", col("qualification_code_coding_explode.code")) \
                                            .withColumn("qualification_code_coding_display", col("qualification_code_coding_explode.display")) \
                                            .withColumn("qualification_code_coding_system", col("qualification_code_coding_explode.system")) \
                                            .drop(col("qualification_code_coding_explode"))\
                                            .withColumn("qualification_code_extension_explode",
                                                        explode_outer(col("qualification_explode.code.extension"))) \
                                            .withColumn("qualification_code_extension_url", col("qualification_code_extension_explode.url")) \
                                            .withColumn("qualification_code_extension_valueboolean",
                                                        col("qualification_code_extension_explode.valueBoolean"))\
                                            .withColumn("qualification_code_extension_extension_explode",
                                                        explode_outer(col("qualification_code_extension_explode.extension"))) \
                                            .withColumn("qualification_code_extension_extension_url",
                                                        col("qualification_code_extension_extension_explode.url")) \
                                            .withColumn("qualification_code_extension_extension_valuecode",
                                                        col("qualification_code_extension_extension_explode.valueCode")) \
                                            .withColumn("qualification_code_extension_extension_valuecodeableconcept_coding_explode", explode_outer(
                                            col("qualification_code_extension_extension_explode.valueCodeableConcept.coding"))) \
                                            .withColumn("qualification_code_extension_extension_valuecodeableconcept_coding_code",
                                                        col("qualification_code_extension_extension_valuecodeableconcept_coding_explode.code")) \
                                            .withColumn("qualification_code_extension_extension_valuecodeableconcept_coding_display",
                                                        col("qualification_code_extension_extension_valuecodeableconcept_coding_explode.display")) \
                                            .withColumn("qualification_code_extension_extension_valuecodeableconcept_coding_system",
                                                        col("qualification_code_extension_extension_valuecodeableconcept_coding_explode.system")) \
                                            .drop(col("qualification_code_extension_extension_valuecodeableconcept_coding_explode")) \
                                            .drop(col("qualification_code_extension_extension_explode")) \
                                            .drop(col("qualification_code_extension_explode")) \
                                            .withColumn("qualification_extension_explode", explode_outer(col("qualification_explode.extension"))) \
                                            .withColumn("qualification_extension_url", col("qualification_extension_explode.url")) \
                                            .withColumn("qualification_extension_valuecode", col("qualification_extension_explode.valueCode")) \
                                            .drop(col("qualification_extension_explode")) \
                                            .withColumn("qualification_identifier_explode", explode_outer(col("qualification_explode.identifier"))) \
                                            .withColumn("qualification_identifier_value", col("qualification_identifier_explode.value")) \
                                            .drop(col("qualification_identifier_explode")) \
                                            .withColumn("qualification_issuer_reference", col("qualification_explode.issuer.reference")) \
                                            .withColumn("PROV_INTRNSHP_FCLTY_NM_prac", when((col('qualificaition_code_coding_code') == "INTERN"),split(col('qualification_issuer_reference'), '/').getItem(1)).otherwise(lit(None))) \
                                            .withColumn("PROV_MED_SCHL_NM_prac", when((col('qualificaition_code_coding_code') == "MEDSCHL"),split(col('qualification_issuer_reference'), '/').getItem(1)).otherwise(lit(None))) \
                                            .withColumn("PROV_RSDNCY_FCLTY_NM_prac", when((col('qualificaition_code_coding_code') == "RESIDEN"),split(col('qualification_issuer_reference'), '/').getItem(1)).otherwise(lit(None))) \
                                            .withColumn("PROVIDER_MEDICAL_SCHOOL_YEAR", when((col('qualificaition_code_coding_code') == "MEDSCHL"), col("qualification_explode.period.end"))) \
                                            .drop(col("qualification_explode"))

            practitioner_df = extract_qual_df.withColumn('name_given_fm', concat_ws(",", col('name_given')))
            split_cols = pyspark.sql.functions.split(practitioner_df['name_given_fm'], ',')
            practitioner_df = practitioner_df.withColumn('FIRST_NAME', split_cols.getItem(0)) \
                                .withColumn('MIDDLE_INITIAL', split_cols.getItem(1)) \
                                .drop(practitioner_df['name_given_fm']) \
                                .drop(practitioner_df['name_given'])
                                
            extract_qual_df =practitioner_df.withColumn("FULL_NAME", concat(coalesce(col("FIRST_NAME"), lit("")), lit(" "),
                                                coalesce(col("MIDDLE_INITIAL"), lit("")), lit(" "),
                                                coalesce(col("LAST_NAME"), lit("")), lit(" ")))

            extract_qual_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_extract_qual_df_hourly/')
            extract_qual_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_extract_qual_df_hourly/')

            qual_df = extract_qual_df.filter((col('qualification_extension_valuecode')!='stlcxx') | (col('qualification_extension_valuecode').isNull()))
            
            stl_list = 'id', 'versionId','qualification_extension_valuecode','qualification_identifier_value'
            state_lic_df = extract_qual_df.filter(extract_qual_df['qualification_extension_valuecode']=='stlcxx') \
                                        .groupBy('id', 'versionId','qualification_extension_valuecode','qualification_identifier_value') \
                                        .agg(*[F.first(x,ignorenulls=True).alias(f'{x}_new') for x in extract_qual_df.columns]).drop(*stl_list)

            
            state_lic_df = state_lic_df.toDF(*(c.replace('_new', '') for c in state_lic_df.columns))
            
            # state_lic_df = extract_qual_df.filter(extract_qual_df['qualification_extension_valuecode']=='stlcxx').groupBy('id', 'versionId', 'qualification_extension_valuecode','qualification_identifier_value').agg(*[F.first(x,ignorenulls=True).alias(x) for x in extract_qual_df.columns if x!=['id', 'qualification_extension_valuecode','qualification_identifier_value']]).drop('id', 'qualification_extension_valuecode','qualification_identifier_value')
            

            diff1 = [c for c in state_lic_df.columns if c not in qual_df.columns]
            diff2 = [c for c in qual_df.columns if c not in state_lic_df.columns]
            qual_df = qual_df.select('*', *[F.lit(None).alias(c) for c in diff1]) \
                .unionByName(state_lic_df.select('*', *[F.lit(None).alias(c) for c in diff2]))
            
            
            qual_df = qual_df.withColumn('PROF_DEGREE_FULL_STD', when((qual_df['qualification_extension_valuecode'] == 'degree'), (qual_df['qualificaition_code_coding_code']))) \
                            .withColumn('qualification_code_extension_valueboolean', F.col('qualification_code_extension_valueboolean').cast(pt.StringType()))
            qual_df = qual_df.withColumn('PROVIDER_BOARD_CER_col', when((qual_df['qualification_extension_valuecode'] == "boardcert"), F.col('qualification_code_extension_valueboolean'))\
                                            .otherwise(F.lit('NA')))

            qual_not_board_df = qual_df.where(~(qual_df.PROVIDER_BOARD_CER_col != 'NA') | (qual_df.PROVIDER_BOARD_CER_col.isNull()))

            board_cert_df = qual_df.where((qual_df.PROVIDER_BOARD_CER_col != 'NA') | (qual_df.PROVIDER_BOARD_CER_col.isNull())).withColumn('PROVIDER_BOARD_CER_col', F.when((F.col('PROVIDER_BOARD_CER_col')=='false'),F.lit('N'))\
                        .otherwise(F.when((F.col('PROVIDER_BOARD_CER_col')=='true'),F.lit('Y')).otherwise(F.lit('N')))) \
                            .withColumn('PROVIDER_BOARD_CER_taxonomy', when((qual_df['qualification_code_coding_system'] == "http://nucc.org/provider-taxonomy"), (qual_df['qualificaition_code_coding_code']))) \
            
            board_cert_df = board_cert_df.withColumn('PROVIDER_BOARD_CER', concat_ws(',', coalesce(col('PROVIDER_BOARD_CER_taxonomy'), lit('')),\
                                coalesce(col('PROVIDER_BOARD_CER_col'), lit('')))).drop('PROVIDER_BOARD_CER_ind', 'PROVIDER_BOARD_CER_taxonomy')
            qual_df = qual_not_board_df.unionByName(board_cert_df, allowMissingColumns=True)

            qual_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_qual_df_hourly/')
            qual_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_qual_df_hourly/')

            board_cert_df = qual_df.select('id', 'versionId', 'PROVIDER_BOARD_CER')
            qual_df = qual_df.drop('PROVIDER_BOARD_CER')

            qualification_df = agg_col_val_format(df=board_cert_df,col_name='PROVIDER_BOARD_CER',col_key='id', join_df=qual_df)

            degree_df = qual_df.select('id', 'versionId', 'PROF_DEGREE_FULL_STD')
            qualification_df = qualification_df.drop('PROF_DEGREE_FULL_STD')
            
            qualification_df = agg_col_val_format(df=degree_df,col_name='PROF_DEGREE_FULL_STD',col_key='id', join_df=qualification_df)
            
            qual_id_df = qualification_df.withColumn('qualification_issuer_reference', when((qualification_df['qualification_code_extension_valueboolean'] == 'true'),
                                                                                            (qualification_df['qualification_issuer_reference']))) \
                                        .withColumn('lic_value',
                                                    when((qualification_df['qualification_extension_valuecode'] == 'stlcxx'), (qualification_df['qualification_identifier_value']))) \
                                        .withColumn('lic_status',
                                                    when((qualification_df['qualification_code_extension_extension_url'] == 'status'), (qualification_df['qualification_code_extension_extension_valuecode']))) \
                                        .withColumn('lic_state', col('qualification_code_extension_extension_valuecodeableconcept_coding_code')) \
                                        .withColumn('lic_degree',
                                                    when((qualification_df['qualification_extension_valuecode'] == 'stlcxx'), (qualification_df['qualificaition_code_coding_code'])))
            
            lic_df = qual_id_df.select('id', 'versionId', 'lic_value', 'lic_status', 'lic_state', 'lic_degree').na.drop(subset=['lic_value', 'lic_status', 'lic_state', 'lic_degree'])

            lic_df = lic_df.withColumn('PROVIDER_LICENSE_DETAILS', concat_ws(',', coalesce(col('lic_value'), lit('')), coalesce(col('lic_status'), lit('')),\
            coalesce(col('lic_state'), lit('')), coalesce(col('lic_degree'), lit('')))).drop('lic_value', 'lic_status', 'lic_state', 'lic_degree')
            
            qual_id_df = qual_id_df.drop('lic_value', 'lic_status', 'lic_state', 'lic_degree')
            qual_id_df = agg_col_val_format(df=lic_df,col_name='PROVIDER_LICENSE_DETAILS',col_key='id', join_df=qual_id_df).drop('qualification_code_extension_extension_url', 'qualification_code_extension_extension_valuecode',\
                'qualification_code_extension_extension_valuecodeableconcept_coding_code', 'qualification_identifier_value')
            
            qual_id_df = qual_id_df.withColumn('seq_col',row_number().over(w.partitionBy("id", 'versionId').orderBy('id')))
            qual_extract_df = qual_id_df.select('id','resourceType',
                                                'GENDER',
                                                'versionId',
                                                'PRACTITIONER_LAST_UPDATE_DATE',
                                                'FIRST_NAME',
                                                'MIDDLE_INITIAL',
                                                'LAST_NAME',
                                                'FULL_NAME',
                                                'NAME_SUFFIX',
                                                'NAME_PREFIX',
                                                'PROVIDER_TITLE',
                                                'PROV_INTRNSHP_FCLTY_NM_prac',
                                                'PROV_MED_SCHL_NM_prac',
                                                'PROV_RSDNCY_FCLTY_NM_prac',
                                                'PROVIDER_MEDICAL_SCHOOL_YEAR',
                                                'seq_col',
                                                'PROVIDER_BOARD_CER',
                                                'PROF_DEGREE_FULL_STD',
                                                'PROVIDER_LICENSE_DETAILS',"year", "month", "day","hour")
            
            extracted_df = df.select('id','identifier',"meta","year", "month", "day","hour").withColumn("identifier_explode", explode_outer(df.identifier)) \
                        .withColumn("identifier_explode_value", col("identifier_explode.value")) \
                        .withColumn("identifier_explode_system", col("identifier_explode.system")) \
                        .withColumn("identifier_explode_period_start", col("identifier_explode.period.start")) \
                        .withColumn("identifier_explode_period_end", col("identifier_explode.period.end"))\
                        .withColumn("identifier_type_coding_explode", explode_outer(col("identifier_explode.type.coding"))) \
                        .withColumn("identifier_type_coding_code", col("identifier_type_coding_explode.code")) \
                        .withColumn("identifier_type_coding_display", col("identifier_type_coding_explode.display")) \
                        .withColumn("identifier_type_coding_system", col("identifier_type_coding_explode.system")) \
                        .drop(col("identifier_type_coding_explode")) \
                        .drop(col("identifier_explode")) \
                        .drop(df.identifier) \
                        .withColumn("versionId", df.meta.versionId) \
                        .drop(df.meta) \
                        
            identifier_df = extracted_df.withColumn('ITIN', when((extracted_df['identifier_type_coding_code'] == 'ITIN'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('SSN', when((extracted_df['identifier_type_coding_code'] == 'SS'), (extracted_df['identifier_explode_value'])).otherwise(lit(None))) \
                                        .withColumn('PMI', when((extracted_df['identifier_explode_system'] == 'http://bpd.bcbs.com/Practitioner/MasterIdentifier'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('PROVIDER_NPI', when((extracted_df['identifier_type_coding_code'] == 'NPI'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('PROVIDER_NUMBER', when((extracted_df['identifier_type_coding_code'] == 'PRN'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('PROV_MEDICARE_NUMBER', when((extracted_df['identifier_type_coding_code'] == 'CCN'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('PROVIDER_X_REFERENCE_value', when((extracted_df['identifier_type_coding_code'] == 'PCRN'), (extracted_df['identifier_explode_value']))) \
                                        .withColumn('PROVIDER_X_REFERENCE_period_start', when((extracted_df['identifier_type_coding_code'] == 'PCRN'), (extracted_df['identifier_explode_period_start']))) \
                                        .withColumn('PROVIDER_X_REFERENCE_period_end', when((extracted_df['identifier_type_coding_code'] == 'PCRN'), (extracted_df['identifier_explode_period_end'])))

            identifier_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_identifier_df_hourly/')
            identifier_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_identifier_df_hourly/')

            x_ref_df = identifier_df.select('id', 'versionId','PROVIDER_X_REFERENCE_value', 'PROVIDER_X_REFERENCE_period_start', 'PROVIDER_X_REFERENCE_period_end').na.drop(subset=['PROVIDER_X_REFERENCE_value'])
        
            x_ref_df = x_ref_df.withColumn('PROVIDER_X_REFERENCE', concat_ws(',', coalesce(col('PROVIDER_X_REFERENCE_value'), lit('')), coalesce(col('PROVIDER_X_REFERENCE_period_start'), lit('')), \
                                        coalesce(col('PROVIDER_X_REFERENCE_period_end'), lit('')))).drop('PROVIDER_X_REFERENCE_value', 'PROVIDER_X_REFERENCE_period_start', 'PROVIDER_X_REFERENCE_period_end')
            identifier_df = identifier_df.drop('PROVIDER_X_REFERENCE_value', 'PROVIDER_X_REFERENCE_period_start', 'PROVIDER_X_REFERENCE_period_end'\
                                                'identifier_explode_period_start', 'identifier_explode_period_end')
            ident_x_ref_df = agg_col_val_format(df=x_ref_df,col_name='PROVIDER_X_REFERENCE',col_key='id', join_df=identifier_df).drop('identifier_explode_period_start', 'identifier_explode_period_end')
            ident_x_ref_df = ident_x_ref_df.withColumn('seq_col',row_number().over(w.partitionBy("id", 'versionId').orderBy('id'))) \
                                            
            ident_extract_df = ident_x_ref_df.select('id','ITIN',
                                                    'SSN',
                                                    'PMI',
                                                    'PROVIDER_NPI',
                                                    'PROVIDER_NUMBER',
                                                    'PROV_MEDICARE_NUMBER',
                                                    'PROVIDER_X_REFERENCE',
                                                    'seq_col',"versionId","year", "month", "day","hour")
            
    ################################### TELECOM ##################################################
            telecom_df = df.select('id', 'telecom',"meta","year", "month", "day","hour").withColumn("loc_telecom", explode_outer(df.telecom)) \
                                                .withColumn("tel_system", col("loc_telecom.system")) \
                                                .withColumn("tel_value", col("loc_telecom.value")) \
                                                .withColumn("tel_use", col("loc_telecom.use")) \
                                                .drop(df.telecom) \
                                                .drop(col("loc_telecom")) \
                                                .withColumn("versionId", df.meta.versionId) \
                                                .drop(df.meta) \
                                                                                    
            ident_x_ref_df = telecom_df
            tel_df = ident_x_ref_df.select('id','versionId','tel_system', 'tel_value','tel_use')
            tel_df = tel_df.withColumn('PROVIDER_PHONE_NUMBER', when((ident_x_ref_df['tel_system'] == 'phone'), (ident_x_ref_df['tel_value']))) \
                            .withColumn('PROVIDER_WEB_ADDRESS', when((ident_x_ref_df['tel_system'] == 'url'), (ident_x_ref_df['tel_value']))) \
                            .withColumn('PROVIDER_FAX_NUMBER', when((ident_x_ref_df['tel_system'] == 'fax'), (ident_x_ref_df['tel_value']))) \
                            .withColumn('PROVIDER_EMAIL_ADDRESS', when((ident_x_ref_df['tel_system'] == 'email'), (ident_x_ref_df['tel_value']))) \
                            .withColumn('PROVIDER_PAGER_NUMBER', when((ident_x_ref_df['tel_system'] == 'pager'), (ident_x_ref_df['tel_value']))) \
                            .withColumn('PROVIDER_SMS_MESSAGE', when((ident_x_ref_df['tel_system'] == 'sms'), (ident_x_ref_df['tel_value'])))
            
            tel_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_tel_df_hourly/')
            tel_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_tel_df_hourly/')

            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_PHONE_NUMBER',col_key='id', join_df=ident_x_ref_df)
            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_WEB_ADDRESS',col_key='id', join_df=ident_x_ref_tel_df)
            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_FAX_NUMBER',col_key='id', join_df=ident_x_ref_tel_df)
            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_EMAIL_ADDRESS',col_key='id', join_df=ident_x_ref_tel_df)
            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_PAGER_NUMBER',col_key='id', join_df=ident_x_ref_tel_df)
            ident_x_ref_tel_df = agg_col_val_format(df=tel_df,col_name='PROVIDER_SMS_MESSAGE',col_key='id', join_df=ident_x_ref_tel_df)
            ident_x_ref_tel_df = ident_x_ref_tel_df.withColumn('PROVIDER_TDD', when(((ident_x_ref_df['tel_system'] == 'other') & (ident_x_ref_df['tel_use']=='work')), (ident_x_ref_df['tel_value'])))
            tel_extract_df = ident_x_ref_tel_df.drop('tel_system', 'tel_value', 'tel_use').distinct().withColumn('seq_col',row_number().over(w.partitionBy("id", 'versionId').orderBy('id')))

    ########################################### COMMUNICATION #################################################################       
            communication_df = df.select('id', 'communication',"meta", "year", "month", "day","hour").withColumn("communication_explode", explode_outer(df.communication)) \
                                            .drop(df.communication) \
                                            .withColumn("communication_coding_explode", explode_outer(col("communication_explode.coding"))) \
                                            .withColumn("communication_coding_code", col("communication_coding_explode.code")) \
                                            .withColumn("communication_coding_display", col("communication_coding_explode.display")) \
                                            .withColumn("communication_coding_system", col("communication_coding_explode.system")) \
                                            .drop(col("communication_coding_explode")) \
                                            .drop(col("communication_explode")) \
                                            .drop(col("communication_coding_display")) \
                                            .withColumn("versionId", df.meta.versionId) \
                                            .drop(df.meta) \
                                            
            communication_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_communication_df_hourly/')
            communication_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_communication_df_hourly/')                                 
            
            qual_id_df = communication_df
            off_lang_df = qual_id_df.select('id', 'versionId', 'communication_coding_system','communication_coding_code')\
                                .withColumn('PROVIDER_LANGUAGE_CODE', when((qual_id_df['communication_coding_system'] == "urn:ietf:bcp:47"), 
                                                                            (qual_id_df['communication_coding_code'])))
            
            comm_extract_df = agg_col_val_format(df=off_lang_df,col_name='PROVIDER_LANGUAGE_CODE',col_key='id', join_df=qual_id_df) \
                                        .drop('communication_coding_system', 'communication_coding_code') \
                                        .distinct() \
                                        .withColumn('seq_col',row_number().over(w.partitionBy("id", 'versionId').orderBy('id'))) \
                                        
    ################################################## EXTENSION #########################################        
            extension_df = df.select('id', 'extension',"meta", "year", "month", "day","hour").withColumn("extension_explode", explode_outer(df.extension)) \
                                            .withColumn('extension_identifier', F.monotonically_increasing_id()) \
                                            .withColumn("extension_extension_explode", explode_outer(col("extension_explode.extension")))\
                                            .withColumn("extension_extension_url", col("extension_extension_explode.url")) \
                                            .withColumn("extension_extension_value_reference", col("extension_extension_explode.valueReference.reference"))\
                                            .withColumn("RECOGNITION_PERIOD_START", col("extension_extension_explode.valuePeriod.start")) \
                                            .withColumn("RECOGNITION_PERIOD_END", col("extension_extension_explode.valuePeriod.end")) \
                                            .withColumn("extension_extension_value_codeable_concept_coding_explode", explode_outer(col("extension_extension_explode.valueCodeableConcept.coding"))) \
                                            .withColumn("extension_extension_value_codeable_concept_coding_code", col("extension_extension_value_codeable_concept_coding_explode.code")) \
                                            .drop(df.extension) \
                                            .drop(col("extension_explode")) \
                                            .drop(col("extension_extension_explode")) \
                                            .drop(col('extension_extension_value_codeable_concept_coding_explode')) \
                                            .withColumn("versionId", df.meta.versionId) \
                                            .drop(df.meta) \
            
            
            extracted_df = extension_df.withColumn('recognizing_Entity', when((extension_df['extension_extension_url'] == 'recognizingEntity'), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                                            .withColumn('recognition_Value', when((extension_df['extension_extension_url'] == 'recognitionValue'), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                                            .withColumn('recognition_Type', when((extension_df['extension_extension_url'] == 'recognitionType'), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                                            .withColumn('recognition_Web_Address', when((extension_df['extension_extension_url'] == 'recognition_Web_Address'), (extension_df['extension_extension_value_codeable_concept_coding_code']))) \
                                            .withColumn('prac_lic_ref', when(element_at(split(extension_df['extension_extension_value_reference'], '-'), -1) == 'Licensee', split(extension_df['extension_extension_value_reference'], '/').getItem(1))) \
                                            .withColumn('prac_ins_ref', when(element_at(split(extension_df['extension_extension_value_reference'], '-'), -1) == 'InsurancePlan', split(extension_df['extension_extension_value_reference'], '/').getItem(1))) \
                                            .drop(col('extension_extension_value_reference'))
            
            extracted_df.write.mode('overwrite').parquet(f's3://{environment_path}/tmp/prac_extracted_df_hourly/')
            extracted_df = spark.read.parquet(f's3://{environment_path}/tmp/prac_extracted_df_hourly/')

            concat_df  = extracted_df.select('id','versionId', 'RECOGNITION_PERIOD_START', 'RECOGNITION_PERIOD_END','recognizing_Entity',\
            'recognition_Value', 'recognition_Type', 'recognition_Web_Address', 'RECOGNITION_PERIOD_START', 'RECOGNITION_PERIOD_END', 'extension_identifier')
            
            concat_df = concat_df.groupBy('id', 'versionId', 'extension_identifier').agg(F.first(concat_df['recognizing_Entity'], True).alias('recognizing_Entity'), \
                                                                            F.first(concat_df['recognition_Value'], True).alias('recognition_Value'), \
                                                                            F.first(concat_df['recognition_Type'], True).alias('recognition_Type'), \
                                                                            F.first(concat_df['recognition_Web_Address'], True).alias('recognition_Web_Address'), \
                                                                            F.first(concat_df['RECOGNITION_PERIOD_START'], True).alias('RECOGNITION_PERIOD_START'), \
                                                                            F.first(concat_df['RECOGNITION_PERIOD_END'], True).alias('RECOGNITION_PERIOD_END')) \
                                                                            .drop('extension_extension_url','extension_extension_value_codeable_concept_coding_code',\
                                                                            'prac_lic_ref', 'prac_ins_ref', 'extension_identifier').na.fill("")
            
            concat_df = concat_df.withColumn('RECOGNITION', concat_ws(',', coalesce(col('recognizing_Entity'), lit('')), coalesce(col('recognition_Value'), lit('')),\
                                coalesce(col('recognition_Type'), lit('')), coalesce(col('RECOGNITION_PERIOD_START'), lit('')), coalesce(col('RECOGNITION_PERIOD_END'), lit('')), coalesce(col('recognition_Web_Address'), lit('')))).drop('RECOGNITION_PERIOD_START', 'RECOGNITION_PERIOD_END','recognizing_Entity',\
                                'recognition_Value', 'recognition_Type', 'recognition_Web_Address')
            
            exten_extract_df = agg_col_val_format(df=concat_df,col_name='RECOGNITION',col_key='id', join_df=extracted_df).select('id', 'RECOGNITION',"versionId",'year', 'month', 'day',"hour").distinct() \
                                            .withColumn('seq_col',row_number().over(w.partitionBy("id", 'versionId').orderBy('id'))) \
            
            exten_extract_df = exten_extract_df.withColumn('RECOGNITION', when((length(col('RECOGNITION')) > 7), col('RECOGNITION')).otherwise(lit(None)))                                
            
            join_key = ['id','seq_col', 'versionId','year', 'month', 'day','hour']
            qua_id_df = qual_extract_df.join(ident_extract_df, join_key, 'outer').dropDuplicates()
            qua_id_tel_df = qua_id_df.join(tel_extract_df, join_key, 'outer').dropDuplicates()
            qua_id_tel_comm_df = qua_id_tel_df.join(comm_extract_df, join_key, 'outer').dropDuplicates()
            join_df = qua_id_tel_comm_df.join(exten_extract_df, join_key, 'outer').dropDuplicates()
    
            col_list = join_df.columns 
            for each_col in col_list:
                if each_col in ['id', 'versionId','year', 'month', 'day','hour']:
                    pass
                else:
                    join_df = join_df.withColumn('flag', F.when(F.col(each_col).isNull(),F.lit(0)).otherwise(F.lit(1)))
                    join_df = join_df.withColumn(each_col , F.last(each_col,True).over(w.partitionBy('id', 'versionId','year', 'month', 'day','hour').orderBy(F.col('flag').desc()).rowsBetween(-sys.maxsize,0))).drop('flag')  
            join_df = join_df.drop('seq_col').distinct()
            df_output = join_df

            print('***************Storing data in temp location ***************')
            output_bucket = args["s3_output_path"]

            df_output = df_output.withColumn('month', F.when(F.length(F.col('month')) == 1, F.concat(F.lit('0'), F.col('month'))).otherwise(F.col('month'))) \
                                .withColumn('day', F.when(F.length(F.col('day')) == 1, F.concat(F.lit('0'), F.col('day'))).otherwise(F.col('day')))\
                                .withColumn('hour', F.when(F.length(F.col('hour')) == 1, F.concat(F.lit('0'), F.col('hour'))).otherwise(F.col('hour')))
            print('no of output records')
        
            df_output.write.mode('overwrite').format('parquet').partitionBy('year', 'month', 'day','hour').option("partitionOverwriteMode", "dynamic").save(output_bucket)
            print('Practitioner Data loaded into partition')

            envName = args['environment'].lower() 
            # Adding raw-zone table partitions

            raw_table = f'bpd-datalake-{envName}-rawzone-hourly-db.rawzone_practitioner_hourly'
            raw_query = f"alter table `{raw_table}` ADD IF NOT EXISTS PARTITION(year={year}, month={month}, day={day}, hour={current_hour}) location '{s3_input_path}year={year}/month={month}/day={day}/hour={current_hour}'"
            run_query(query=raw_query, output_location=f's3://bpd-datalake-{envName}-zone/athena/')
            print(f'added partitions for {raw_table}')

            # Adding flatten zone table partitions

            table_name = f'bpd-datalake-{envName}-flattenzone-hourly-db.flattenzone_practitioner_hourly'
            query = f"msck repair table `{table_name}`"
            run_query(query=query, output_location=f's3://bpd-datalake-{envName}-zone/athena/')
            print(f'added partitions for {table_name}')

        else:
            print(f'Practitioner data not available at source - {current_pdate} for hour -{current_hour }')
        print('Checking the current_pdate')
        
        return current_pdate ,current_hour


    if __name__ == "__main__":
        run_date = datetime.today()
        current_pdate = None
        glueContext = GlueContext(SparkContext.getOrCreate())
        spark = glueContext.spark_session
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        current_pdate,current_hour = process_data(spark, last_date,last_hour)
        current_pdate = datetime.strptime((current_pdate).strftime('%Y-%m-%d'),"%Y-%m-%d").date()
        
        # Prepare metadata table
        print("Initiating processing for metadata table")
        processing_time = datetime.now()
        job_name_met = 'practitioner'
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
        message = (f' Job_Name: {job_name}\n Status: {status} \n environment: {environment} \n Job_Run_ID: {job_run_id} \n Run_Date: {run_date} \n ERROR: {e}')
    subject = (f'[{environment}] {job_name} {status}')
    sns_client.publish(TopicArn=sns_arn,
        Message=message,
        Subject=subject)
    raise e