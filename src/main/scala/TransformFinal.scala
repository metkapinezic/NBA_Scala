import org.apache.spark.sql.functions.{col, explode, when, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformFinal {
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
    def readMatchesData(spark: SparkSession, gamesDF: DataFrame): DataFrame = {
      import spark.implicits._

      // Explode the data in the DataFrame to flatten it
      val gameDFexplode = gamesDF.select(explode($"data"))
        .withColumn("game_id", $"col.id")
        .withColumn("home_team_name", $"col.home_team.full_name")
        .withColumn("home_team_id", $"col.home_team.id")
        .withColumn("home_team_score", $"col.home_team_score")
        .withColumn("visitor_team_name", $"col.visitor_team.full_name")
        .withColumn("visitor_team_id", $"col.visitor_team.id")
        .withColumn("visitor_team_score", $"col.visitor_team_score")
        .drop("col")

      gameDFexplode
    }

    def readStatsData(spark: SparkSession, path: os.Path): DataFrame = {
      val files = os.walk(path).filter(os.isFile).toSeq
      val dfSeq = files.flatMap { file =>
        val json = ujson.read(os.read(file))
        json("data").arr.flatMap { stats =>
          val gameId = stats("game")("id").num.toInt // Change 'matchId' to 'gameId'
          val teamId = stats("team")("id").num.toInt
          val playerId = stats("player")("id").num.toInt
          val playerName = s"${stats("player")("first_name").str} ${stats("player")("last_name").str}"
          val points = stats("pts").num.toInt
          Seq((gameId, teamId, playerId, playerName, points)) // Change 'matchId' to 'gameId'
        }
      }

      import spark.implicits._
      dfSeq.toDF("game_id", "team_id", "player_id", "player_name", "points") // Change 'match_id' to 'game_id'
    }

    // Read statistics data from JSON files in Filtered_stats directory
    val statsPath = os.pwd / "Games_folder" / "Filtered_stats"
    val statsDF = readStatsData(spark, statsPath)

    // Join matches data and statistics data on game ID and team ID
    val joinedDF = joinData(readMatchesData(spark, gamesDF), statsDF)

    // Save the final DataFrame to a CSV file with headers and handle missing data in 'nb_steals'
    val outputPath = os.pwd / "Games_folder" / "final_data.csv"
    saveToCSV(joinedDF.na.fill(0, Seq("nb_steals")), outputPath)

    spark.stop()
  }

  def joinData(matchesDF: DataFrame, statisticsDF: DataFrame): DataFrame = {
    import matchesDF.sparkSession.implicits._

    val filteredMatchesDF = matchesDF.select(
      col("game_id"),
      col("home_team_name"),
      col("home_team_id"),
      col("home_team_score"),
      col("visitor_team_name"),
      col("visitor_team_id"),
      col("visitor_team_score")
    )

    val filteredStatisticsDF = statisticsDF.select(
      col("game_id"), // Use col() instead of $"match_id"
      col("team_id"),
      col("player_name"),
      col("points")
    )

    val joinedDF = filteredMatchesDF
      .join(filteredStatisticsDF, Seq("game_id"))
      .withColumn("team", when(col("team_id") === col("home_team_id"), col("home_team_name")).otherwise(col("visitor_team_name")))
      .withColumn("nb_steals", lit(0)) // Adding a new column 'nb_steals' with a constant value 0
      .drop("home_team_id", "visitor_team_id")

    joinedDF
  }

  def saveToCSV(df: DataFrame, path: os.Path): Unit = {
    df.write
      .format("csv")
      .option("header", "true")
      .save(path.toString)
  }
}

