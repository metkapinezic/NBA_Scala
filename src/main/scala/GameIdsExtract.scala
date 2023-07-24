import os._

object GameIdsExtract {
  def main(args: Array[String]): Unit = {
    val wd = os.pwd / "Games_folder" / "Filtered_games"

    val gameIDsFilePath = wd / "GameIDs.txt" // Path for the GameIDs.txt file

    os.walk(wd)
      .filter(os.isFile)
      .foreach { file =>
        val json = ujson.read(os.read(file))
        json("data").arr.foreach { game =>
          val gameId = game("id").num.toInt
          os.write.append(gameIDsFilePath, s"$gameId\n")
        }
      }

    println("Game IDs have been written to GameIDs.txt.")
  }
}
