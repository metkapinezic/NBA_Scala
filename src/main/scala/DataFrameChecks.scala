import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFrameChecks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("DataFrameChecks")
      .getOrCreate()

    // Read games data from JSON files in Filtered_games directory
    val gamesPath = os.pwd / "Games_folder" / "Filtered_games"
    val gamesDF = spark.read.json(s"$gamesPath/*.json")

    // Read statistics data from JSON files in Filtered_stats directory
    val statsPath = os.pwd / "Games_folder" / "Filtered_stats"
    val statsDF = readStatsData(spark, statsPath)

    // Check columns and schema of gamesDF
    println("Columns in gamesDF:")
    gamesDF.columns.foreach(println)

    println("\nSchema of gamesDF:")
    gamesDF.printSchema()

    // Check columns and schema of statsDF
    println("\nColumns in statsDF:")
    statsDF.columns.foreach(println)

    println("\nSchema of statsDF:")
    statsDF.printSchema()

    spark.stop()
  }

  // Mock implementation of readStatsData function to avoid external dependencies
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
}
