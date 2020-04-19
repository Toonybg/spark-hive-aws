package com.amarkovski

import org.apache.spark.sql.SparkSession

object RequetesSQL extends App{

  //1. create spark session with hive warehouse configuration
  val warehouseLocation = "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/hive"

  val spark = SparkSession
    .builder()
    .appName("Spark Hive SQL")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()




  //2. Pour chaque utilisateur, le nombre d'Animés différents qu'il a regardé
  val animCount = spark.sql("SELECT username, count(distinct(anime_id)) FROM AnimUserList GROUP BY username")
  animCount.show()

  //3. Pour chaque utilisateur, l'Animé qu'il a le plus regardé
  val mostWatchedByUser = spark.sql(
    "SELECT username, anime_id, max(nbAnime) as watchCount " +
    "FROM ( SELECT username, anime_id,,count(anime_id) as nbAnime " +
           "FROM AnimUserList " +
           "GROUP BY username, anime_id "  +
    "GROUP BY username")
  mostWatchedByUser.show()

  //4. Pour chaque Animé, l'épisode le plus regardé
  val mostWatchedEpisode = spark.sql(
    "SELECT anime_id,episode_id, max(nbEpisode) as watchCount " +
    "FROM ( SELECT anime_id, episode_id,count(episode_id) as nbEpisode " +
           "FROM AnimUserList " +
           "GROUP BY anime_id, episode_id ) " +
    "GROUP BY episode_id ")
  mostWatchedEpisode.show()

  //5. L'utilisateur qui a regardé le plus d’Animés
  val userWatchedMostAnimes = spark.sql(
    "SELECT username, max(nbAnime) " +
      "FROM (SELECT username, count(distinct(anime_id)) as nbAnime " +
            "FROM AnimUserList " +
            "GROUP BY username )"
  )
  userWatchedMostAnimes.show()

  //6. L'utilisateur qui a regardé le plus d’épisodes
  val userWatchedMostEpisodes = spark.sql(
    "SELECT username, max(nbEpisodes) " +
      "FROM ( SELECT username, sum(my_watched_episodes) as nbEpisodes " +
              "FROM AnimUserList " +
              "GROUP BY username )"
    )
  userWatchedMostEpisodes.show()

  //7. Nombre d'utilisateurs différents? #302675
  val nbUsers = spark.sql("SELECT count(distinct(user_id)) AS nbUsers FROM UserList")
  nbUsers.show()

  //8. Les 100 utilisateurs qui ont donné les moyennes de notes les plus élevées.
  val bestScore = spark.sql(
    "SELECT username, avg(rating) as avgRating" +
      "FROM AnimUserList " +
      "GROUP BY username " +
      "ORDER BY avgRating" +
      "LIMIT 100")
  bestScore.show()

  //9.
}
