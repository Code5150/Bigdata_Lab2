package com.vladislav

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lower, trim}
import org.apache.spark.{SparkConf, SparkContext}

case class LangYear(lang: String, year: Int)

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    Logger.getLogger("org.apache.parquet").setLevel(Level.WARN)

    val appName = "BigdataLab2"
    val master = "local[2]"

    val config = new SparkConf().setAppName(appName).setMaster(master).setAll(List.apply(
      ("spark.driver.memory", "45g"),
      ("spark.executor.memory", "6g"),
      ("spark.driver.maxResultSize", "8g")
    ))
    val sc = new SparkContext(config)
    val spark = SparkSession.builder().appName(appName).config(config).getOrCreate()

    val postsPath = "C:/Users/Vladislav/Desktop/Big data/L2/Posts.xml"
    val langPath = "C:/Users/Vladislav/Desktop/Big data/L2/programming_languages.csv"
    val reportPath = "C:/Users/Vladislav/IdeaProjects/bigdata_lab2_2/report.parquet"

    val langDf = spark.read.option("header", value = true).csv(langPath)
    val languages = langDf.withColumn("name", trim(lower(langDf("name"))))
      .select("name").rdd.map(_.getString(0)).collect().toSet

    val postsDf = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load(postsPath)

    import spark.implicits._

    postsDf.createOrReplaceTempView("posts")

    println("posts created")

    val postsByYearAndTagWithCount = spark.sql("SELECT _Id as id, _Tags as tags, EXTRACT(year from _CreationDate) as year FROM posts " +
      "WHERE _CreationDate BETWEEN cast('2010-01-01' as timestamp) and cast('2021-01-01' as timestamp) " +
      "and _Tags is not null").rdd.flatMap(
        row => row.getString(1).strip().drop(1).dropRight(1)
          .split("><")
          .map(s => (LangYear(s, row.getInt(2)), 1L)))
      .reduceByKey(_ + _)
      .filter(l => languages.contains(l._1.lang))
      .map(s => (s._1.year, s._1.lang, s._2))
      .toDF("year", "language", "count")

    println("tags counted")

    postsByYearAndTagWithCount.createOrReplaceTempView("postsYearLangCount")

    val result = (2010 until 2021).map(
      i => spark.sql(s"SELECT * FROM postsYearLangCount WHERE year = $i ORDER BY year ASC, count DESC LIMIT 10")
    ).reduce((df1, df2) => df1.union(df2))

    result.show(100)
    result.write.parquet(reportPath)

    sc.stop()
  }
}
