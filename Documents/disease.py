#Connect to s3
import spark

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>>")
df = spark.read.option("header", "True").csv("s3://dpo2/input-data/disease.csv")
df.show()
df.printSchema()


# to check null, nan values
from pyspark.sql.functions import col,isnan, when, count

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]
   ).show()

#remove extra spaces form the data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
df_trimmed = df.withColumn("Disease_ID", trim(col(" Disease_ID")))
df_trimmed.show()

df2 = df_trimmed.drop(" Disease_ID")
df2.show()

#count for duplicates values

df2.count()

df3 = df2.distinct()
df3.count()

# Connect to Redshift

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>")


# Write data to redshift table
df2.write.format("redshift").option("url", "").option("dbtable", "project.disease").option("driver","").option("tempdir", "").option("user", "").option("password", "").option("aws_iam_role", "arn:aws:iam:").mode("overwrite").save()