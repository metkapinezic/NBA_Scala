import java.io.PrintWriter

object TeamIdsExtract {
  def main(args: Array[String]): Unit = {
    val wd = os.pwd / "Games_folder" / "All_games"

    // Set to keep track of teams already printed
    val printedTeams = scala.collection.mutable.Set.empty[String]

    // Map to store team names and IDs
    val teamIdsMap = scala.collection.mutable.Map.empty[String, Int]

    // Set of team names to filter
    val teamNamesToFilter = Set("Phoenix Suns", "Atlanta Hawks", "LA Clippers", "Milwaukee Bucks")

    // Read and process each file
    os.list(wd).foreach { file =>
      val json = ujson.read(os.read(file))
      val gamesData = json("data")

      // Extract and print the Full name and ID for each game if it matches the specified teams
      gamesData.arr.foreach { game =>
        val homeTeam = game("home_team")
        val visitorTeam = game("visitor_team")
        val homeTeamFullName = homeTeam("full_name").str
        val homeTeamID = homeTeam("id").num.toInt
        val visitorTeamFullName = visitorTeam("full_name").str
        val visitorTeamID = visitorTeam("id").num.toInt

        // Check if either the home team or visitor team is in the set of team names to filter
        if (teamNamesToFilter.contains(homeTeamFullName)) {
          // Print the team if it has not been printed yet
          if (!printedTeams.contains(homeTeamFullName)) {
            println(s"$homeTeamFullName (ID: $homeTeamID)")
            printedTeams.add(homeTeamFullName)
            teamIdsMap.put(homeTeamFullName, homeTeamID)
          }
        }

        if (teamNamesToFilter.contains(visitorTeamFullName)) {
          // Print the team if it has not been printed yet
          if (!printedTeams.contains(visitorTeamFullName)) {
            println(s"$visitorTeamFullName (ID: $visitorTeamID)")
            printedTeams.add(visitorTeamFullName)
            teamIdsMap.put(visitorTeamFullName, visitorTeamID)
          }
        }
      }
    }

    // Create a file path for the txt file
    val outputPath = os.pwd / "Games_folder" / "All_games" / "TeamIDs.txt"

    // Write the printed teams and their IDs to the txt file
    val writer = new PrintWriter(outputPath.toIO)
    teamIdsMap.foreach { case (teamName, teamID) =>
      writer.println(s"$teamName (ID: $teamID)")
    }
    writer.close()
  }
}


