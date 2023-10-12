# NBA_Scala

The NBA Data ETL Project aims to extract and transform NBA game data stored in JSON format. The project utilizes Apache Spark and the Scala programming language to perform data transformation, join operations, and generate a final output in CSV format. The goal is to provide a clear and organized summary of NBA game statistics, including the total points scored by each player and the number of steals recorded in each game based on the following requerements:
 - request HTTP APIs
 - collect the matches for the 2021-2022 season of the following teams: Phoenix Suns, Atlanta Hawks, Los Angeles Clippers, Milwaukee Bucks
 - collect the statistics of the selected matches upstream
 - generate 2 DataFrames on matches and statistics
 - merge both DataFrames obtained and export it in a csv file

Steps which achieved the output:
1. src/main/scala/GamesExtractAll.scala --> Extracts json files for all games data in Games_folder/All_games 
2. src/main/scala/TeamIdsExtract.scala --> Extracts TeamIDs.txt
4. src/main/scala/GamesSeasonTeamsExtract.scala --> Extract json files in Games_folder/Filtered_games based on requerements
5. src/main/scala/GameIdsExtract.scala --> Exstracts GameIDs.txt
6. src/main/scala/StatsSeasonGamesExtract.scala --> Extracts filtered statistics data based on requreded game ids 
7. src/main/scala/TransformFinal.scala --> transforms the extracted and filtered json files in dataframe and outputs Games_folder/final_data.csv

Process Overview:

Data Ingestion: The project starts by reading NBA game data from JSON files stored in the "Games_folder/Filtered_games" directory. The data is then flattened using the explode function to create a DataFrame containing individual game records.

Statistics Data: NBA player statistics data is extracted from JSON files in the "Games_folder/Filtered_stats" directory. The data is processed to extract relevant information such as game ID, team ID, player ID, player name, and points scored.

Data Joining: The project performs a join operation on the flattened game data and player statistics data based on the common columns "game_id" and "team_id." This join operation combines the information from both DataFrames to create a unified dataset.

Data Transformation: The project performs column renaming to ensure consistency across the dataset. Columns like "home_team" are renamed to "home_team_name," and similarly, "home_team_id" is renamed to "home_team_id," etc.

Data Enrichment: A new column "nb_steals" is introduced to the dataset with a constant value of 0 to indicate the number of steals, as this information was not available in the source data.

CSV Output: The final transformed dataset is saved to a CSV file named "final_data.csv" in the "Games_folder" directory. The CSV file contains columns such as "game_id," "home_team_name," "home_team_id," "home_team_score," "visitor_team_name," "visitor_team_id," "visitor_team_score," "player_id," "total_pts_scored," and "nb_steals."

Usage:

Prerequisites: Working in IntellJ, need to have have Apache Spark and Scala installed on your machine.

Data Preparation: Place the NBA game data in JSON format under the "Games_folder/Filtered_games" directory and the player statistics data in JSON format under the "Games_folder/Filtered_stats" directory.

Run the Project: Open the IntelliJ project and execute the TransformFinal object's main method. The program will read the data, perform data transformation and enrichment, and generate the final output CSV file "final_data.csv" in the "Games_folder" directory.

Output Evaluation: Examine the "final_data.csv" file to access the processed and enriched NBA game data. The CSV file includes columns representing game details, team information, player statistics, and additional data on points scored and the number of steals.
