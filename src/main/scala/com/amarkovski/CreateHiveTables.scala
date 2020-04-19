package com.amarkovski

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CreateHiveTables extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.WARN)
  val spark = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    //.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  //read userlist into dataframe
  println("***START*********")
  val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("datasets/UserAnimeList100.csv")
  //create temp table (in hive)
  df.createOrReplaceTempView("UserAnimeList100")
  //df.show()
  df.printSchema()
  //create UserAnimeList Table in hive
  df.write.mode("overwrite").saveAsTable("amarkovskidb.UserAnimeList")

  /* alternative
  spark.sql("CREATE TABLE IF NOT EXISTS UserAnimeList" +
    " (username string, anime_id int, my_watched_episodes int," +
    "my_start_date string, my_finish_date string, my_score int," +
    "my_status int, my_rewatching int, my_rewatching_ep int," +
    "my_last_updated int, my_tags string )")
  //save table to hive
   */

  val ual = spark.read.format("csv").option("header","true").option("inferSchema","true").load("UserAnimeList100.csv")
  ual.createOrReplaceTempView("UserAnimeList")

  val al = spark.read.format("csv").option("header","true").option("inferSchema","true").load("AnimeList100.csv")
  al.createOrReplaceTempView("AnimeList")


  val ul = spark.read.format("csv").option("header","true").option("inferSchema","true").load("UserList.csv")
  ul.createOrReplaceTempView("UserList")

  val oneDF = spark.sql(
    "SELECT  ual.username, ul.gender, ul.location, ul.birth_date, ual.anime_id, al.title_english, al.duration, al.rating, al.score, al.rank, ual.my_watched_episodes, ual.my_score, ual.my_status, ual.my_tags " +
      "FROM UserAnimeList AS ual " +
      "LEFT JOIN AnimeList AS al ON ual.anime_id = al.anime_id " +
      "LEFT JOIN UserList AS ul ON ual.username = ul.username")
  oneDF.printSchema()
  oneDF.write.mode("overwrite").saveAsTable("amarkovskidb.JoinedUserAnimeList")


  println("***END*********")
}
