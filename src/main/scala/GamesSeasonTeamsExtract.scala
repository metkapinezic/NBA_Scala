import org.apache.spark.sql.SparkSession

object GamesSeasonTeamsExtract {
  def main(args: Array[String]): Unit = {
    // Combine the team IDs into a comma-separated string
    val teamIds = "1,13,24,17"

    val spark = SparkSession.builder().config("spark.master", "local").appName("NBA").getOrCreate()
    val filename = "filtered_games"
    val wd = os.pwd / "Games_folder" / "Filtered_games"
    os.makeDir.all(wd)

    teamIds.split(",").foreach { teamId =>
      val url = s"https://www.balldontlie.io/api/v1/games?seasons[]=2021&team_ids[]=$teamId"
      val firstPage = requests.get(url)
      val firstJson = ujson.read(firstPage.text)
      val totalPages = firstJson("meta")("total_pages").num.toInt

      for (page <- 1 until totalPages + 1) {
        val r = requests.get(url + s"&page=$page")
        val json = ujson.read(r.text)
        os.write(wd / s"$filename${teamId}_$page.json", ujson.write(json))
        println(s"Saved file $filename${teamId}_$page.json")

        // Print the content of the JSON file
        println(json)

        spark.stop()
      }
    }
  }
}
