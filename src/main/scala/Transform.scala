import org.apache.spark.sql.functions.{col, explode, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Transform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("NBA_Data")
      .getOrCreate()

    // Read the game data from JSON files into a DataFrame
    val gamesPath = os.pwd / "Games_folder" / "Filtered_games"
    val gamesDF = spark.read.json(s"$gamesPath/*.json")

    // Define the functions to read matches and statistics data
    def readMatchesData(spark: SparkSession, path: os.Path): DataFrame = {
      import spark.implicits._

      // Explode the data in the DataFrame to flatten it
      val gameDFexplode = gamesDF.select(explode($"data"))
        .withColumn("match_id", $"col.id")
        .withColumn("homeTeam", $"col.home_team.full_name")
        .withColumn("homeTeamId", $"col.home_team.id")
        .withColumn("homeTeamScore", $"col.home_team_score")
        .withColumn("visitorTeam", $"col.visitor_team.full_name")
        .withColumn("visitorTeamId", $"col.visitor_team.id")
        .withColumn("visitorTeamScore", $"col.visitor_team_score")
        .drop("col")

      gameDFexplode
    }

    def readStatsData(spark: SparkSession, path: os.Path): DataFrame = {
      val files = os.walk(path).filter(os.isFile).toSeq
      val dfSeq = files.flatMap { file =>
        val json = ujson.read(os.read(file))
        json("data").arr.flatMap { stats =>
          val matchId = stats("game")("id").num.toInt
          val teamId = stats("team")("id").num.toInt
          val playerId = stats("player")("id").num.toInt
          val playerName = s"${stats("player")("first_name").str} ${stats("player")("last_name").str}"
          val points = stats("pts").num.toInt
          Seq((matchId, teamId, playerId, playerName, points))
        }
      }

      import spark.implicits._
      dfSeq.toDF("match_id", "team_id", "player_id", "player_name", "points")
    }

    // Read statistics data from JSON files in Filtered_stats directory
    val statsPath = os.pwd / "Games_folder" / "Filtered_stats"
    val statsDF = readStatsData(spark, statsPath)

    // Join matches data and statistics data on match ID and team ID
    val joinedDF = joinData(readMatchesData(spark, gamesPath), statsDF)

    // Save the final DataFrame to a CSV file
    val outputPath = os.pwd / "Games_folder" / "final_data.csv"
    saveToCSV(joinedDF, outputPath)

    spark.stop()
  }

  def joinData(matchesDF: DataFrame, statisticsDF: DataFrame): DataFrame = {
    import matchesDF.sparkSession.implicits._

    val filteredMatchesDF = matchesDF.select("match_id", "homeTeam", "homeTeamId", "visitorTeam", "visitorTeamId")
    val filteredStatisticsDF = statisticsDF.select("match_id", "team_id", "player_name", "points")

    val joinedDF = filteredMatchesDF
      .join(filteredStatisticsDF, Seq("match_id"))
      .withColumn("team", when(col("team_id") === col("homeTeamId"), col("homeTeam")).otherwise(col("visitorTeam")))
      .drop("homeTeamId", "visitorTeamId")

    joinedDF
  }

  def saveToCSV(df: DataFrame, path: os.Path): Unit = {
    df.write.csv(path.toString)
  }
}