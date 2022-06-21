package com.gitpractice
import org.apache.spark.sql.SparkSession

object InitialGitInteg {
  def main(args:Array[String]): Unit={
    //creating spark session
    val spark:SparkSession=SparkSession
      .builder().master("local[*]")
      .appName("DATA FROM AWS S3")
      .getOrCreate()


    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIAXWVFJ5UJAAFXQ25Y")
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "qzFVEwX2vwbwA0S2KDEbe9CxMHfk6qfnesRwmPYj")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    //Step 2 - Read the products.csv
    val ds = spark.read
      .option("header",true)
      .option("delimiter",",")
      .csv("s3a://ramadata/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")

    ds.show()

    ds.write.format("CSV")
      .option("header",true)
      .save("s3a://ramadata/outputnewd/")

    // ds.show()

  }

}
