from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr, col, column
import argparse
import sys
import time
import datetime
import pandas as pd
from pyspark import SparkContext
import re
import csv
import logging


class InvalidNoOfArgError(Exception):

    def __init__(self, args):
        self.msg = args


def spark_execute(EXT_QRY_PATH, AUD_QRY_PATH, EXT_HDFS_PATH, ZIP_FLAG):
    spark = 0

    try:

        logging.info("creating spark session")

        spark = (SparkSession.builder

                 .master("yarn")

                 .config(conf=SparkConf()
                         .set("spark.port.maxRetries", 25)
                         .set("spark.yarn.queue", "scheduled.etl")
                         .set("spark.driver.cores", "3")
                         .set("spark.driver.memoryOverhead", "10g")
                         .set("spark.executor.cores", "3")
                         .set("spark.executor.memory", "20g")
                         .set("spark.executor.memoryOverhead", "10g")
                         .set("spark.executor.instances", 100)
                         .set("spark.dynamicAllocation.maxExecutors", 150)
                         .set("spark.ui.port", "4080")
                         .set("spark.rdd.compress", "true")
                         .set("spark.network.timeout", "10000001s")
                         .set("spark.sql.broadcastTimeout", "10000s")
                         .set("spark.sql.shuffle.partitions", 200)
                         )
                 .enableHiveSupport()
                 .getOrCreate()

                 )

        logging.info("spark session created")

    # logging.info("spark session created")

    except Exception as e:

        logging.error("could not create spark session due to exception")

        logging.exception(e)

        sys.exit(1)

    try:

        logging.info("reading extraction query")
        e_query = spark.read.text(EXT_QRY_PATH)
        et_query = e_query.toPandas()
        logging.info("reading audit query")
        a_query = spark.read.text(AUD_QRY_PATH)
        at_query = a_query.toPandas()

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', -1)

        Keys = args.SUBS_KEY_SET.split("&")
        Values = args.SUBS_VAL_SET.split("&")
        eo_query = et_query.to_string(header=False, index=False)
        ao_query = at_query.to_string(header=False, index=False)

        for i in range(len(Keys)):
            eo_query = eo_query.replace(Keys[i], Values[i])
            ao_query = ao_query.replace(Keys[i], Values[i])

        # eo_query = et_query.value.str.replace('<<SALT_VALUE>>',args.SALT_VALUE).str.replace('<<PROCESS_DATE>>',str(PROC_DT)[0:10]).str.replace('<<SALT_DATE>>',args.SALT_DATE)

        logging.info("***********************Transformed Extraction query*****************************")
        logging.info(eo_query)
        logging.info("********************************************************************************")
        logging.info("***********************Transformed Audit query**********************************")
        logging.info(ao_query)

    except Exception as e:

        logging.info("Couldn't found or transform the sql")
        logging.error(e)
        sys.exit(1)

    try:

        logging.info("running extraction spark sql")
        e_df = spark.sql(eo_query)
        logging.info("spark sql executed and dataframe got created")

    except Exception as e:

        logging.error("Extraction spark sql failed due to exception")
        logging.exception(e)
        sys.exit(1)

    if ZIP_FLAG.upper() == 'N':

        try:

            logging.info("writing data frame into csv file")
            e_df.repartition(4).write.csv(EXT_HDFS_PATH, header=False, mode='overwrite', sep='', nullValue='\u0000',
                                          emptyValue='\u0000')
            logging.info("data frame got written into csv files")

        except Exception as e:

            logging.error("writing dataframe into csv file failed")
            logging.exception(e)
            sys.exit(1)

    elif ZIP_FLAG.upper() == 'Y':

        try:

            logging.info("Writing data frame to zipped csv files")
            e_df.repartition(4).write.csv(EXT_HDFS_PATH, compression="gzip", header=False, mode='overwrite', sep='',
                                          nullValue='\u0000', emptyValue='\u0000')
            logging.info("data frame got written into zipped csv files")

        except Exception as e:

            logging.error("writing dataframe into zipped csv file failed")
            logging.exception(e)
            sys.exit(1)

    try:

        logging.info("running audit spark sql")
        a_df = spark.sql(ao_query)
        logging.info("spark sql executed and dataframe got created")

    except Exception as e:

        logging.error("Audit spark sql failed due to exception")

    # df1 = spark.read.format("csv").option("header", "false").load(EXT_HDFS_PATH+"/*.csv")

    logging.info("****************Getting count from extract files*****************")
    df1 = spark.read.format("csv").option("header", "false").load(EXT_HDFS_PATH + "/*.csv*")
    extract_cnt = df1.count()
    table_cnt = [i for i in a_df.collect()[0]][0]
    logging.info("*************************Count of Extract file*******************")
    logging.info("Extraction count {0}".format(extract_cnt))
    logging.info("*************************Count of Audit query********************")
    logging.info("Source table count {0}".format(table_cnt))

    if int(extract_cnt) == int(table_cnt):

        logging.info("Audit count matched successfully")

    else:

        logging.info("Audit Count not matched")


# df.coalesce(1).write.format("com.databricks.spark.csv").save("/user/dw/gopal/debit_hsh")


def main():
    try:

        if len(sys.argv) == 13:

            parser = argparse.ArgumentParser()
            parser.add_argument("CATEGORY", help="Provide category name to Pyspark SQL")
            parser.add_argument("PROCDATE", help="Provide IWDATE to Pyspark SQL")
            ##parser.add_argument("SALT_VALUE",help="Provide Salt Value to Pyspark SQL")
            ##parser.add_argument("SALT_DATE",help="Provide Salt Date to Pyspark SQL")
            parser.add_argument("ACTIVE_FLAG", help="Provide active flag status for this data category")
            parser.add_argument("ZIP_FLAG", help="Provide Zip flag status for this data category")
            parser.add_argument("FREQUENCY", help="Provide extraction frequency for this data category")
            parser.add_argument("DAY_OF_WEEK", help="Provide extraction frequency for this data category")
            parser.add_argument("DATE_OF_MONTH", help="Provide extraction frequency for this data category")
            parser.add_argument("INP_EXT_QRY", help="Provide extraction query file for this data category")
            parser.add_argument("INP_AUD_QRY", help="Provide audit query file for this data category")
            parser.add_argument("OP_EXT_PATH", help="Provide extraction output path for this data category")
            parser.add_argument("SUBS_KEY_SET",
                                help="Provide variable names which needs to be replaced in query for this data category")
            parser.add_argument("SUBS_VAL_SET",
                                help="Provide variable values which needs to be replaced in query for this data category")

            global args

            args = parser.parse_args()

            logging.info("***Below are the Arguments passed***")

            for attr, value in args.__dict__.items():
                logging.info('{0}----{1}'.format(attr, value))

        else:

            logging.error("12 arguments are needed but {0} are passed".format(len(sys.argv) - 1))

            raise InvalidNoOfArgError("Please check no of arguments passed")



    except InvalidNoOfArgError as e:

        logging.exception(e)

        sys.exit(1)

    global PROC_DT

    PROC_DT = datetime.datetime.strptime(args.PROCDATE, '%Y-%m-%d')

    if args.ACTIVE_FLAG.upper() == "N":

        logging.info(
            "Active flag is set to {}. So skipping extraction for {} data category".format(args.ACTIVE_FLAG.upper(),
                                                                                           args.CATEGORY))

        sys.exit(0)

    else:

        logging.info("Active flag is set to Y. So continuing extraction..")

    if args.FREQUENCY.upper() == "DAILY":

        logging.info("Extraction frequency is set to {}..so continuing extraction..".format(args.FREQUENCY.upper()))



    elif args.FREQUENCY.upper() == "WEEKLY":

        logging.info("Extraction frequency is set to {}..now checking day of week ...".format(args.FREQUENCY.upper()))

        if args.DAY_OF_WEEK.upper() == PROC_DT.strftime('%A').upper():

            logging.info(
                "{} extraction is set to run on {}..so continuing extraction..".format(args.DAY_OF_WEEK.upper()))

        else:

            logging.info(
                "{} extraction is not set to run on {}..so skipping extraction and exiting from script.".format(
                    args.CATEGORY, PROC_DT.strftime('%A').upper()))

            sys.exit(0)

    elif args.FREQUENCY.upper() == "MONTHLY":

        logging.info("Extraction frequency is set to {}..now checking date of month ...".format(args.FREQUENCY.upper()))

        if args.DATE_OF_MONTH == PROC_DT.strftime('%d').upper():

            logging.info(
                "{} extraction is set to run on {} date of month..so continuing extraction..".format(args.CATEGORY,
                                                                                                     PROC_DT.strftime(
                                                                                                         '%d').upper()))

        else:

            logging.info(
                "{} extraction is not set to run on {} date of month..so skipping extraction and exiting from script..".format(
                    args.CATEGORY, PROC_DT.strftime('%d').upper()))

            sys.exit(0)

    else:

        logging.info("{} is invalid entry for frequency..exiting script with error".format(args.FREQUENCY.upper()))

        sys.exit(0)

    logging.info("calling spark_execute function")

    spark_execute(args.INP_EXT_QRY, args.INP_AUD_QRY, args.OP_EXT_PATH, args.ZIP_FLAG)


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename='/home/e110682/updated/dw_spark_ext.log',
                    filemode='a', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
    # sys.exit(main())

    main()
