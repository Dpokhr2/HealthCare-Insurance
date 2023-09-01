# Connect to AWS

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>>")


# Reading data from dataframe
subscriberdf = spark.read.option("header", "True").csv("s3://dpo2/input-data/subscriber.csv")
subscriberdf.show()
subscriberdf.printSchema()

# To see if there is any null values
from pyspark.sql.functions import col,isnan, when, count

subscriberdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in subscriberdf.columns]
   ).show()

# Replace null values with 'NA'
df1 = subscriberdf.na.fill("NA")
df1.show()

#Trim job
from pyspark.sql.functions import col, trim
df_trimmed = df1.withColumn("sub_id", trim(col("sub _id"))) \
    .withColumn("ZipCode", trim(col("Zip Code")))
df_trimmed.show()

#Drop duplicate column
df2 = df_trimmed.drop("sub _id") \
.drop("Zip Code")
df2.show()

# To find duplicates

df2.count()
df3 = df2.distinct()
df3.count()

# Write the data into s3 (Optional)

df.write.csv("s3://")

# Connect to redshift

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Access Key>>")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Secret Key>>")


df2.write.format("redshift").option("url", "").option("dbtable", "project.claims").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://").option("user", "").option("password", "").option("aws_iam_role", "").mode("overwrite").save()


