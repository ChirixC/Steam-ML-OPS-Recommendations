import pandas as pd
import numpy as np
from multiprocessing import Pool
import matplotlib.pyplot as plt
import seaborn as sns
import math
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors

FILE_NAMES = ['ml1_games_for_model.parquet','ml2_users_for_model.parquet']
ml1_games = pd.read_parquet(FILE_NAMES[0])

def get_similar_games(game_id, df):
    game_features = df.loc[game_id].values.reshape(1, -1)
    cos_sim = cosine_similarity(game_features, df.values)
    sim_indices = cos_sim.argsort()[0][-6:-1]
    sim_indices = sim_indices[::-1]
    sim_names = df.index[sim_indices]
    return list(sim_names)

# x= get_similar_games(10,ml1_games)

ml_game_df = pd.read_parquet(FILE_NAMES[0])
ml2_user_df = pd.read_parquet(FILE_NAMES[1])

ml2_user_df.drop('genres',axis=1,inplace=True)
ml2_user_df.drop('avg_price',axis=1,inplace=True)
ml2_user_df

def get_nonzero_columns(df, user_id):

    user_row = df.loc[df['user_id'] == user_id]

    nonzero_cols = df.columns[user_row.any()]
  
    nonzero_list= nonzero_cols.tolist()

    return nonzero_list

def clean_list(lst):
  # Create an empty list to store the cleaned items
  cleaned = []
  # Loop through each item in the original list
  for item in lst:
    # If the item ends with _like, remove the _like part
    print(item)
    if item.endswith("_like") or (item == 'user_id'):
      continue
    # If the item is not already in the cleaned list, append it
    if item not in cleaned:
      cleaned.append(item)
  # Return the cleaned list
  return cleaned

def find_top_games(df,features):
  # Select only the columns that match the features
  # print(df)
  print(features)
  df_features = df[features]
  # Compute the cosine similarity matrix
  sim_matrix = cosine_similarity(df_features, df_features)
  # Get the indices of the top 5 games for each game
  top_indices = sim_matrix.argsort(axis=1)[:, -6:-1]
  # Reverse the order to get the most similar first
  top_indices = top_indices[:, ::-1]
  # Create a new dataframe with the game ids and the top 5 games
  df_top_games = pd.DataFrame(df["game_id"])
  for i in range(5):
    df_top_games[f"top_{i+1}"] = df["game_id"].iloc[top_indices[:, i]].values
  # Return the new dataframe
  return df_top_games.iloc[0]

# pass_list = clean_list(user_col_important)
# top = find_top_games(ml_game_df, pass_list)
# top