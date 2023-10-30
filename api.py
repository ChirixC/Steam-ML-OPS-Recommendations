from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# import pyarrow as pa
import pyarrow.parquet as pq
from ast import literal_eval
import pandas as pd
import json as js
import os

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/")
def index():
    return {"message": "Esta es una prueba y si vas a link/pam hay un secreto"}

@app.get("/pam")
def index():
    return {"message": "♥ ❤ ♥"}

@app.get("/developer/{developer}")
def developer(developer: str):
    # Read the parquet file into a Pandas DataFrame
    developers = pd.read_parquet('query1.parquet')

    # Search for developers with the name "Alice"
    developers_year = developers.query("developer == @developer")
    developers_year

    df_sum = developers_year.groupby(developers_year['release_date'].dt.year)['price'].agg(['sum', 'count'])
    df_json = literal_eval(df_sum.to_json(orient='index'))
    return df_json



@app.get("/userdata/{user_id}")
def userdata(user_id: str):
    def read_parquets ():
        query_2_api = pd.read_parquet('api_query2_new.parquet')
        return query_2_api

    def get_user_data(user_id,reviews_dataframe):
        user_data = reviews_dataframe.query("user_id == @user_id")
        user_waste = user_data["total_waste"].iloc[0]
        recommendation = user_data["reviews_percent"].iloc[0]
        total_reviews = float(user_data["total_reviews"].iloc[0])

        print(total_reviews)
        print(type(total_reviews))

        json = {user_id: { "spent_money":user_waste, "recommendation %":recommendation, "reviews_amount":total_reviews}}
        return json
    
    reviews_data = read_parquets()
    json_output = get_user_data(user_id, reviews_data)
           

    return json_output
    
@app.get("/UserForGenre/{genre}")
def UserForGenre(genre: str):
    def read_parquets ():
        query_3_most_played = pd.read_parquet('api_querery3_most_played_genre.parquet')
        query_3_years_genre = pd.read_parquet('api_query3_years_genre.parquet')
        return query_3_most_played,query_3_years_genre
    
    def top_player(genre,most_played_df):
        player_data = most_played_df.query("genre == @genre")
        player_id = player_data["user_id"].iloc[0]
        played_time = player_data["total_played_time"].iloc[0]
        return player_id,played_time

    def played_per_year(genre, years_genre_df): 
        years_data = years_genre_df.query("genres == @genre")
        years_data.drop(columns=['id_list'],axis=1, inplace=True)
        years_dic = {}
        outer_year_dic = {}

        for i in years_data:
            year = int(i)
            year_value = int(years_data[i].iloc[0])
            if year_value > 0:
                years_dic[year] = year_value
            
        outer_year_dic["Years"] = years_dic
        return outer_year_dic
    
    most_played_df, years_genre_df = read_parquets()
    player_id, played_time = top_player(genre,most_played_df)
    years_dic = played_per_year(genre,years_genre_df)
  
    years_json = js.dumps(years_dic)
    years_json = js.loads(years_json)

    output_json = {"User With Most Played time para ": genre, "User": player_id, "Played_Time":played_time, "Hours Genre Was Played": years_json}
    return output_json

@app.get("/best_developer_year/{year}")
def best_developer_year(year: int):
    def read_parquets ():
        top_dev_df = pd.read_parquet('api_query4_top_developes.parquet')
        return top_dev_df
    
    def bring_top(top_dev_df, year):
        top_devs_year_data = top_dev_df.nlargest(3,f'{year}')
        top_devs_filt = top_devs_year_data[str(year)]
        respose_list = []

        for i in top_devs_filt.index:
            respose_list.append([i])         
        return respose_list
    
    top_dev_df = read_parquets()

    if year > 2009 and year <2022:
        json_list = bring_top(top_dev_df, year)
        json_response = {"First Place":json_list[0] , "Second Place":json_list[1], "Third Place":json_list[2]}
    else:
        json_response = {"message": f"Year {year} wasn't a great year for game try for years in range 2010-2021"}
    return json_response

@app.get("/developer_reviews_analysis/{developer}")
def best_developer_year(developer: str):
    def read_parquets ():
        dev_sentiment_df = pd.read_parquet('api_query5_dev_sent.parquet')
        return dev_sentiment_df
    
    def retrieve_dev_sent(dev_sentiment_df,developer):
        dev_data = dev_sentiment_df.query("developer == @developer")
        positive = (dev_data['Positive'])
        negative = (dev_data['Negative'])
        positive_value= positive.iloc[0]
        negative_value= negative.iloc[0]
        return positive_value,negative_value

    dev_sentiment_df = read_parquets()
    positive_value,negative_value = retrieve_dev_sent(dev_sentiment_df, developer)
    json_response = {f"{developer}": {"Positive": positive_value, "Negative": negative_value}}
    return json_response