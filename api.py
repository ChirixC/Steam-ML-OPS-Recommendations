from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# import pyarrow as pa
# import pyarrow.parquet as pq
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


# clients = pq.read_table('user_items_df.parquet')
# clients_pd = pa.Table.to_pandas(clients)

# def get_items_amount(user):
#     # Debería checkar si esta lista tiene duplicados
#     items = clients_pd.loc[clients_pd['user_id'] == user, 'items']  
#     items2 = items[0]
#     return items2.size

# @app.get('/user/')
# def get_client_items(user_id: str):
#     count = get_items_amount(user_id)
#     return {"result": count}

# @app.get('/clients/{user}')
# def get_client_items(user):
#     count = get_items_amount(user)
#     return count

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
        query_2_items = pd.read_parquet('query_2_items.parquet')
        query_2_reviews = pd.read_parquet('query_2_reviews.parquet')
        return query_2_steam, query_2_items, query_2_reviews

    # Cantidad de dinero gastado por el usuario

    def get_items_names(dataframe,user_id):
        return dataframe.query("user_id == @user_id")


    def get_items_id(row_df):
        items= []
        user_items = list(row_df['items'].values)[0]

        for item in user_items: 
            items.append(item['item_id'])

        return items

    def get_waste(items_list):
        waste = 0
        prices = []
        for item in items_list:
            intintem=int(item)
            price = query_2_steam.query("id == @intintem")
            # prices.append((price['price'].values))

            if len(price['price'].values) > 0:
                waste = waste+price['price'].values[0]
                prices.append((price['price'].values))
        return waste


    def percent_reviews(user_id):
        try:
            recommends = query_2_reviews.query("user_id == @user_id")
            recommends_true = query_2_reviews.query("(user_id == @user_id) &(recommend == True)")
            # print(recommends)
            items_amount = len(recommends)
            percent = (len(recommends_true) * 100) / items_amount
        
            # print(percent)
            return percent, items_amount
        except ZeroDivisionError:
            return 0
    
    query_2_steam, query_2_items, query_2_reviews = read_parquets()

    user_row = get_items_names(query_2_items,user_id)
    items_list = get_items_id(user_row)    
    total_waste = get_waste(items_list)
    reviews_items_percent, total_reviews = percent_reviews(user_id)

    json_output = {user_id: { "spent_money":total_waste, "recommendation %":reviews_items_percent, "reviews_amount":total_reviews}}


    return json_output
    