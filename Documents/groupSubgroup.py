# Connect to AWS

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>>")


# Reading data from dataframe
df = spark.read.option("header", "True").csv("s3://dpo2/input-data/grpsubgrp.csv")
df.show()
df.printSchema()

# To see if there is any null values
from pyspark.sql.functions import col,isnan, when, count

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]
   ).show()

# To find duplicates

df.count()
df3 = df.distinct()
df3.count()

# Write the data into s3 (Optional)

df.write.csv("s3://")

# Connect to redshift

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>>")


df2.write.format("redshift").option("url", "").option("dbtable", "project.claims").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://").option("user", "").option("password", "").option("aws_iam_role", "").mode("overwrite").save()


