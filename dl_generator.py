from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

def generate_data():
    df = spark.createDataFrame([("Computer", "Computer1", 1000)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("append").save("delta/products")
    df = spark.createDataFrame([("Computer", "Computer2", 1500)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("append").save("delta/products")
    df = spark.createDataFrame([("Computer", "Computer3", 2000)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("append").save("delta/products")
    df = spark.createDataFrame([("Computer", "Computer1", 1000),("Computer", "Computer3", 2000)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("overwrite").save("delta/products")
    df = spark.createDataFrame([("Peripheral", "Mouse1", 15), ("Peripheral", "KeyBoard1", 20)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("append").save("delta/products")
    df = spark.createDataFrame([("Computer", "Computer1", 1000),("Computer", "Computer3", 1200),("Peripheral", "Mouse1", 10), ("Peripheral", "KeyBoard1", 20), ("Peripheral", "KeyBoard2", 200)]).toDF("cat", "name", "price")
    df.write.format("delta").mode("overwrite").save("delta/products")

def generate_data2():
    df = spark.createDataFrame([("Black", "Computer1")]).toDF("color", "name")
    df.write.format("delta").mode("append").save("delta/colors")
    df = spark.createDataFrame([("Red", "Computer2")]).toDF("color", "name")
    df.write.format("delta").mode("append").save("delta/colors")
    df = spark.createDataFrame([("Black", "Computer3")]).toDF("color", "name")
    df.write.format("delta").mode("append").save("delta/colors")
    df = spark.createDataFrame([("Black", "Computer1"), ("Red", "Computer3")]).toDF("color", "name")
    df.write.format("delta").mode("overwrite").save("delta/colors")
    df = spark.createDataFrame([("Black", "Mouse1"), ("Red", "KeyBoard1")]).toDF("color", "name")
    df.write.format("delta").mode("append").save("delta/colors")
    df = spark.createDataFrame([("Black", "Computer1"), ("Red", "Computer3"),("Black", "Mouse1"), ("Black", "KeyBoard1"), ("Green", "KeyBoard2")]).toDF("color", "name")
    df.write.format("delta").mode("overwrite").save("delta/colors")

def generate_data_if_needed():
    if not(os.path.isdir("./delta")):
        generate_data()
        generate_data2()