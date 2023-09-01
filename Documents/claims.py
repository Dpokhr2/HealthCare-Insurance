# Access to s3
import spark #Not needed if running from a databrick notebook

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")

#Reading it from dataframe
df2 = spark.read.option("header", "True").json("s3://dpo2/input-data/claims.json")
df2.show()
df2.printSchema()

#Count the number of rows
df2.count()

#Count distinct number of rows
df3 = df2.distinct()
df3.count()

#Write the clean data in S3 (Optional)
df2.write.csv("<s3 file path>")

#Connecting to redshift

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")

# Writing the data in redshift

df2.write.format("redshift").option("url", "").option("dbtable", "project.claims").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://").option("user", "").option("password", "").option("aws_iam_role", "").mode("overwrite").save()
