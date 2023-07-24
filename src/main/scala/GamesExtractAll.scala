import org.apache.spark.sql.SparkSession

object GamesExtractAll {
    def main(args: Array[String]): Unit = {
      val url = "https://www.balldontlie.io/api/v1/games"
      val spark = SparkSession.builder().config("spark.master", "local").appName("NBA_DATA").getOrCreate()
      val filename = "all_games"
      val firstPage = requests.get(url)
      val firstJson = ujson.read(firstPage.text)
      val totalPages = firstJson("meta")("total_pages").num.toInt

      // Create the "Games_folder" / "All_games" directory
      val wd = os.pwd / "Games_folder" / "All_games"
      os.makeDir.all(wd)

      for (page <- 1 to totalPages) {
        val r = requests.get(url + s"?page=$page")
        val json = ujson.read(r.text)
        os.write(wd / filename.concat(s"_$page.json"), ujson.write(json))

        spark.stop()
      }
    }
}