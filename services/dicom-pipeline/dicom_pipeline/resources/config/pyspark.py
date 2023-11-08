spark_conf = {
    "spark.jars.packages": ",".join(
        [
            "net.snowflake:snowflake-jdbc:3.8.0",
            "net.snowflake:spark-snowflake_2.12:2.8.2-spark_3.0",
            "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7",
        ]
    ),
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
    "spark.hadoop.fs.s3.buffer.dir": "/tmp",
}

PYSPARK_WRITE_MODE = "overwrite" #| overwrite | append
