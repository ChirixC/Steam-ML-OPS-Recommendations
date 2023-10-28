from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# import pyarrow as pa
import pyarrow.parquet as pq
from ast import literal_eval
import pandas as pd
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
        query_2_steam = pd.read_parquet('query_2_steam.parquet')
        query_2_reviews = pd.read_parquet('query_2_reviews.parquet')
        return query_2_steam, query_2_reviews

# Cantidad de dinero gastado por el usuario

    def load_parquet_in_batches(file_path,user_id,chunk_size=5000):
        """Loads a Parquet file in batches.

        Args:
            file_path: The path to the Parquet file.
            chunk_size: The size of each batch.

        Returns:
            A list of Pandas DataFrames.
        """
        parquet_file = pq.ParquetFile(file_path)
        dataframes = []
        for batch in parquet_file.iter_batches(batch_size=5000):
            dataframes = batch.to_pandas()
            result = get_items_names(dataframes,user_id)
            if result is not None :
                return result
        return result

    
    def get_items_names(items_dataframe,user_id):
        items_dataframe = items_dataframe.set_index("user_id")    
        if user_id in items_dataframe.index:
            found_data_frame = items_dataframe.loc[user_id]

            if not found_data_frame.empty:
                print('entra4')
                return found_data_frame
        else:
            return None
        return None


    def get_items_id(row_df):
        items= []
        user_items = row_df.iloc[0]

        for item in user_items:
            items.append(item['item_id'])
        return items

    def get_waste(items_list):
        waste = 0
        prices = []
        for item in items_list:
            intintem=int(item)
            price = query_2_steam.query("id == @intintem")

            if len(price['price'].values) > 0:
                waste = waste+price['price'].values[0]
                prices.append((price['price'].values))
        return waste


    def percent_reviews(user_id):
        try:
            recommends = query_2_reviews.query("user_id == @user_id")
            recommends_true = query_2_reviews.query("(user_id == @user_id) &(recommend == True)")
            items_amount = len(recommends)
            percent = (len(recommends_true) * 100) / items_amount
            return percent, items_amount
        except ZeroDivisionError:
            return 0,0
        
    # Load small Parquets
    query_2_steam, query_2_reviews = read_parquets()

    # Load the Parquet file in batches
    query_2_items = load_parquet_in_batches('query_2_items.parquet',user_id)

    items_list = get_items_id(query_2_items)  

    total_waste = get_waste(items_list)
    reviews_items_percent, total_reviews = percent_reviews(user_id)

    json_output = {user_id: { "spent_money":total_waste, "recommendation %":reviews_items_percent, "reviews_amount":total_reviews}}
    json_output


    return json_output
    
@app.get("/UserForGenre/{genre}")
def UserForGenre(genre: str):

    Global_Max_User_id = ''
    Global_Max_Time_Played = 0

    def read_parquets ():
        genre_grouped_steam = pd.read_parquet('genre_grouped.parquet')
        return genre_grouped_steam

    def load_parquet_in_batches(file_path,Global_Max_Time_Played, Global_Max_User_id):
        """Loads a Parquet file in batches.

        Args:
        file_path: The path to the Parquet file.
        chunk_size: The size of each batch.

        Returns:
        A list of Pandas DataFrames.
        """
        parquet_file = pq.ParquetFile(file_path)
        dataframes = []
        for batch in parquet_file.iter_batches(batch_size=5000):
            dataframes = batch.to_pandas()
            Global_Max_Time_Played, Global_Max_User_id= check_global_user(dataframes,Global_Max_Time_Played, Global_Max_User_id)
        return Global_Max_Time_Played, Global_Max_User_id

    def get_frame_genre(genre_grouped_steam,genre):  
        genre_grouped_steam = genre_grouped_steam.set_index("genres")  
        if genre in genre_grouped_steam.index:
            found_data_frame = genre_grouped_steam.loc[genre]  # esto lo debería hacer una sola vez afuera
            print('LEN ', len(found_data_frame['id_list']))
            return found_data_frame
        else:
            return None
        

    def genre_check(genre_grouped_steam,item_id) :      
            return float(item_id) in genre_grouped_steam['id_list']


    def check_global_user (batch_user_items,Global_Max_Time_Played, Global_Max_User_id):
        for row in batch_user_items.iterrows() :

            data_row = row[1]
            user_time_played = 0
            user_id_row = data_row['user_id']
            items_row = tuple(data_row['items'])

            for item in items_row:

                if genre_check(genre_grouped_steam,item['item_id']):
                    user_time_played += float(item['playtime_forever']) 
            
            if user_time_played > Global_Max_Time_Played:
                Global_Max_Time_Played = user_time_played
                Global_Max_User_id = user_id_row
        return Global_Max_Time_Played, Global_Max_User_id
            
    genre_grouped_steam = read_parquets()
    genre_grouped_steam = get_frame_genre(genre_grouped_steam,genre)

    if genre_grouped_steam: Global_Max_Time_Played, Global_Max_User_id = load_parquet_in_batches('query_2_items.parquet',Global_Max_Time_Played, Global_Max_User_id)

    print(Global_Max_Time_Played)
    print(Global_Max_User_id)

    return {Global_Max_User_id,Global_Max_Time_Played}