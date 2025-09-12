# file: main.py
from extract import extract_player_game_log, extract_team_game_log

# example values (replace with your runtime inputs)
player_df = extract_player_game_log(player="Josh Allen", position="QB", season=2022)
team_df   = extract_team_game_log(team="Kansas City Chiefs", season=1995)

# save raw extracts if you want (still part of Extract)
player_df.to_csv("data/raw/josh_allen_2022.csv", index=False)
team_df.to_csv("data/raw/chiefs_1995.csv", index=False)
