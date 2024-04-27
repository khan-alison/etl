from pyspark.sql import SparkSession
import pyspark.sql.functions as F

appName = "PySpark Example - MariaDB Example"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .config("spark.jars", "jars/mariadb-java-client-3.3.0.jar, jars/postgresql-42.7.3.jar") \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

sql = "select * from HumanResources_Employee"
database = "u960615773_adventureworks"
user = "u960615773_lkp"
password = "Leanhduc@1234"
server = "154.56.38.47"
port = 3306
local_port = 5432
local_user = "root"
local_password = "password"
jdbc_url = f"jdbc:mysql://{server}:{port}/{database}?permitMysqlScheme"
jdbc_driver = "org.mariadb.jdbc.Driver"
pg_url = f"jdbc:postgresql://localhost:{local_port}/etl_practice"
pg_properties = {
    "user": local_user,
    "password": local_password,
    "driver": "org.postgresql.Driver"
}

# Create a data frame by reading data from Oracle via JDBC
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", sql) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbc_driver) \
    .load()

cols_df = [
    "BusinessEntityID",
    "NationalIDNumber",
    "LoginID",
    "OrganizationNode",
    "OrganizationLevel",
    "JobTitle",
    "BirthDate",
    "MaritalStatus",
    "Gender",
    "HireDate",
    "SalariedFlag",
    "VacationHours",
    "SickLeaveHours",
    "CurrentFlag",
    "rowguid",
    "ModifiedDate"
]

df_output = df.select(cols_df) \
    .withColumn("MaritalStatus", F.when(df["MaritalStatus"] == "M", "Married")
                .when(df["MaritalStatus"] == "S", "Single").otherwise("Other")) \
    .withColumn("Gender", F.when(df["Gender"] == "M", "Male")
                .when(df["Gender"] == "F", "Female").otherwise("Other"))

df_output.show()
df_output.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "HumanResources_Employee") \
    .options(**pg_properties) \
    .mode("append").save()

df.show()
spark.stop()
