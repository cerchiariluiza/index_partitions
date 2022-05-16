import datetime
import json
from pprint import pprint
from pyspark.sql.functions import lit
import boto3
import pyspark.sql.functions as f 
from pyspark.sql import Window
import sys
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType
# #inicio job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
environment = 'der' 
AWS_REGION = "us-east-1"

#config datas
dt_yesterday_referencia = datetime.datetime.now() - datetime.timedelta(days=1)
year_yesterday_referencia = dt_yesterday_referencia.year
month_yesterday_referencia = str(dt_yesterday_referencia.month).zfill(2)
day_yesterday_referencia= str(dt_yesterday_referencia.day).zfill(2)
date_yesterday_referencia = "%s%s%s" % (year_yesterday_referencia,  month_yesterday_referencia, day_yesterday_referencia)
date_yesterday_referencia_query = "%s%s%s%s%s%s" % (year_yesterday_referencia,"-",month_yesterday_referencia,"-",day_yesterday_referencia, '%')
date_today_processamento  = datetime.datetime.now() - datetime.timedelta(days=0)
year_today_processamento = date_today_processamento.year
month_today_processamento = str(date_today_processamento.month).zfill(2)
day_today_processamento = str(date_today_processamento.day).zfill(2)
date_today_processamento = "%s%s%s" % (year_today_processamento, month_today_processamento, day_today_processamento)

# #parameter store tipos de bonus
AWS_REGION = "us-east-1"
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
investimento_parametrizado = ssm_client.get_parameter(Name='I',WithDecryption=False)
fopa_parametrizado = ssm_client.get_parameter(Name='F',WithDecryption=False)
# boleto_parametrizado = ssm_client.get_parameter(Name='M',WithDecryption=False)
rede_parametrizado = ssm_client.get_parameter(Name='R',WithDecryption=False)

# # #base_outros_bonus
# select = glueContext.create_dynamic_frame.from_catalog(database = "banco_bonus", table_name = "receptor")
# select.toDF().createOrReplaceTempView("view")
# query= "SELECT * FROM view"
# busca = spark.sql(query)

# Digitos de 0 a 9  a partir agencia 5
### Digitos de 0 a partir agencia 5
#job_seletor_digito_0_range_agencia_a_partir_5.py
database = "banco_bonus"
tbcadastral = "tb___topicos_cadastral"
tbbonus = "tb_boletos_filtrador"
digito = '0'
faixa_agencia_ate_4 = '4'
faixa_agencia_a_partir_de_5 = '5'
partition_cadastral_digito ="numero_digito_verificador_ like '%{}%' and partition_agencia >='{}'".format(digito,faixa_agencia_a_partir_de_5)


datasource_cadastral_digito = glueContext.create_dynamic_frame.from_catalog(
    database=database, 
    table_name=tbcadastral,
    transformation_ctx="datasource_cadastral_digito",
    push_down_predicate=partition_cadastral_digito,
   Additional_options={"catalogPartitionPredicate":partition_cadastral_digito, "boundedFiles" : "5" })


datasource_bonus_digito = datasource_cadastral_digito.toDF()
datasource_bonus_digito.show(10)
