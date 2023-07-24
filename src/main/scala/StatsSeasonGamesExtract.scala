import org.apache.spark.sql.SparkSession
import os._

object StatsSeasonGamesExtract {
  def main(args: Array[String]): Unit = {
    // Spark Session
    val spark = SparkSession.builder().config("spark.master", "local").appName("NBA_Data").getOrCreate()

    // Input and output directories
    val inputDir = os.pwd / "Games_folder" / "Filtered_games"
    val outputDir = os.pwd / "Games_folder" / "Filtered_stats"
    os.makeDir.all(outputDir)

    // Read game IDs from GameIDs.txt
    val gameIDsFilePath = inputDir / "GameIDs.txt"
    val gameIDs = os.read.lines(gameIDsFilePath)

    gameIDs.foreach { gameId =>
      val url = s"https://www.balldontlie.io/api/v1/stats?seasons[]=2021&game_ids[]=$gameId"
      val r = requests.get(url)
      val json = ujson.read(r.text)
      val fileName = s"Filtered_stats_$gameId.json"
      os.write(outputDir / fileName, ujson.write(json))
      println(s"Saved file $fileName")
    }

    spark.stop()
  }
}

