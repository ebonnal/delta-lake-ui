import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages io.delta:delta-core_2.11:0.1.0 pyspark-shell"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
from pyspark.sql import SparkSession
import pyspark
import re

spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()


def get_used_tables(statement):
    matches = re.findall("(FROM|JOIN) ?([^ ]+)", statement)
    return [match[1].replace(".", "/") for match in matches]

def format_table_path(table):
    return table.replace('/', '_')




    # from datetime import datetime

    # dt_obj = datetime.strptime('20.12.2016 09:38:42,76',
    #                        '%d.%m.%Y %H:%M:%S,%f')
    # millisec = dt_obj.timestamp() * 1000
    #
    # print(millisec)

def run(statement):
    # Request treatment
    tables = get_used_tables(statement)
    for table in tables:
        statement = statement.replace(table.replace("/", "."), f"global_temp.{format_table_path(table)}")
    data = []
    version_max = 0
    stack = ""
    try:
        while True:
            for table in tables:
                print(table, format_table_path(table))
                spark.read.format("delta").option("versionAsOf", version_max).load(table).createOrReplaceGlobalTempView(format_table_path(table))
                print(table, format_table_path(table))
            print(version_max, statement)
            try:
                data.append([version_max, spark.sql(statement).limit(1).collect()[0][0]])
            except Exception as e:
                data.append([version_max, None])
                stack = str(e)
                break
            version_max += 1
    except pyspark.sql.utils.AnalysisException as e:
        if "time travel" not in str(e):
            stack = str(e)

    return stack if len(stack) else data