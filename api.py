from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# import pyarrow as pa
import pyarrow.parquet as pq
from ast import literal_eval
import pandas as pd
import json as js
from model import get_similar_games

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
    return {"message": "Welcome try our /docs"}

@app.get("/developer/{developer}")
async def developer(developer: str):
    """
    Get the count and free percent of items by year for a given developer.

    Parameters
    ----------
    developer : str
        The name of the developer to be searched.

    Returns
    -------
    dict
        A dictionary with the year as the key and a sub-dictionary with the count and free percent as the values. For example:
        {
            2020: {
                "count": 10,
                "free_percent": 20.0
            },
            2021: {
                "count": 15,
                "free_percent": 33.33
            }
        }
        If the developer name is invalid, return a dictionary with a message key and an error message as the value. For example:
        {
            "message": "Invalid developer name. Please try again."
        }
    """
    # Read the parquet file into a Pandas DataFrame
    developers = pd.read_parquet('Data/API/api_query1.parquet')

    # Validate Developer
    if developer not in developers['developer'].unique():
        return {"message": "Invalid developer name. Please try again."}
    
    # Search for developers
    developers_year = developers.query("developer == @developer")
 
    # Group by year
    df_grouped = developers_year.groupby(developers['release_date'].dt.year).agg(
    count = ("developer", "count"), # count of items for that year
    free_percent = ("price", lambda x: round((x == 0).sum() / x.count() * 100, 2)) # percent of free items in price for that year
    )

    df_json = literal_eval(df_grouped.to_json(orient='index'))
    return df_json



@app.get("/user_data/{user_id}")
async def userdata(user_id: str):
    def read_parquets ():
        """
        This function reads a parquet file from a local directory and returns a pandas dataframe.

        Returns
        -------
        pandas.DataFrame
        A dataframe containing the data from the parquet file.
        """
        query_2_api = pd.read_parquet('../Data/API/api_query2.parquet')
        return query_2_api

    def get_user_data(user_id,reviews_dataframe):

        """
        This function queries a dataframe of reviews by user_id and returns a JSON object with some statistics.

        Parameters
        ----------
        user_id : str
        The user_id to query the reviews dataframe.
        reviews_dataframe : pandas.DataFrame
        The dataframe of reviews with columns: user_id, total_waste, reviews_percent, total_reviews.

        Returns
        -------
        dict
        A JSON object with the user_id as the key and a nested dictionary as the value. The nested dictionary contains the following keys and values:
            - spent_money: the total_waste value for the user_id.
            - recommendation %: the reviews_percent value for the user_id.
            - reviews_amount: the total_reviews value for the user_id.
        """
        user_data = reviews_dataframe.query("user_id == @user_id")
        user_waste = user_data["total_waste"].iloc[0]
        recommendation = user_data["reviews_percent"].iloc[0]
        total_reviews = float(user_data["total_reviews"].iloc[0])

        print(total_reviews)
        print(type(total_reviews))

        json = {user_id: { "spent_money":user_waste, "recommendation %":recommendation, "reviews_amount":total_reviews}}
        return json
    
    
    reviews_data = read_parquets()
    
    if user_id not in reviews_data['user_id'].unique():
        return {"message": "Invalid User. Please try again."}
    
    json_output = get_user_data(user_id, reviews_data)
           

    return json_output
    
@app.get("/user_for_genre/{genre}")
async def UserForGenre(genre: str):
    def read_parquets ():
        """
        This function reads two parquet files from a local directory and returns two pandas dataframes.

        Returns
        -------
        tuple of pandas.DataFrame
        A tuple containing two dataframes: query_3_most_played and query_3_years_genre. The first dataframe contains the data from the parquet file api_query3_most_played_genre.parquet, which has columns: user_id, genre, total_played_time. The second dataframe contains the data from the parquet file api_query3_years_genre.parquet, which has columns: genres, id_list, and one column for each year from 2010 to 2020.
        """
        query_3_most_played = pd.read_parquet('.\\Data\\API\\api_query3_most_played_genre.parquet')
        query_3_years_genre = pd.read_parquet('.\\Data\\API\\api_query3_years_genre.parquet')
        return query_3_most_played,query_3_years_genre
    
    def top_player(genre,most_played_df):
        """
        This function queries a dataframe of most played genres by user_id and returns the user_id and total_played_time of the top player for a given genre.

        Parameters
        ----------
        genre : str
        The genre to query the most played dataframe.
        most_played_df : pandas.DataFrame
        The dataframe of most played genres by user_id with columns: user_id, genre, total_played_time.

        Returns
        -------
        tuple of (str, int)
        A tuple containing the user_id and total_played_time of the top player for the given genre.
        """
        player_data = most_played_df.query("genre == @genre")
        player_id = player_data["user_id"].iloc[0]
        played_time = player_data["total_played_time"].iloc[0]
        return player_id,played_time

    def played_per_year(genre, years_genre_df): 
        """
        This function queries a dataframe of genres played per year and returns a dictionary with the years and the number of games played for a given genre.

        Parameters
        ----------
        genre : str
        The genre to query the years genre dataframe.
        years_genre_df : pandas.DataFrame
        The dataframe of genres played per year with columns: genres, id_list, and one column for each year from 2010 to 2020.

        Returns
        -------
        dict
        A dictionary with the key "Years" and the value as another dictionary with the keys as the years and the values as the number of games played for the given genre.
        """
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
    
    # Reads Files
    most_played_df, years_genre_df = read_parquets()

    # In case a not valid value is passed
    if genre not in most_played_df['genre'].unique():
        return {"message": "Invalid User. Please try again."}
    
    # Look for top players and time played
    player_id, played_time = top_player(genre,most_played_df)
    years_dic = played_per_year(genre,years_genre_df)
  
    # Formats Json
    years_json = js.dumps(years_dic)
    years_json = js.loads(years_json)

    output_json = {"User With Most Played time para ": genre, "User": player_id, "Played_Time":played_time, "Hours Genre Was Played": years_json}
    return output_json

@app.get("/best_developer_year/{year}")
async def best_developer_year(year: int):
    def read_parquets ():
        """
        This function reads a parquet file from a local directory and returns a pandas dataframe.

        Returns
        -------
        pandas.DataFrame
        A dataframe containing the data from the parquet file api_query4_top_developes.parquet, which has columns: developer, and one column for each year from 2010 to 2020 with the number of games developed by that developer in that year.
        """
        top_dev_df = pd.read_parquet('\Data\API\api_query4_top_developes.parquet')
        return top_dev_df
    
    def bring_top(top_dev_df, year):
        """
        This function finds the top three developers with the most games developed in a given year and returns a list of their names.

        Parameters
        ----------
        top_dev_df : pandas.DataFrame
        The dataframe of developers with the number of games developed per year with columns: developer, and one column for each year from 2010 to 2020.
        year : int
        The year to filter the dataframe by.

        Returns
        -------
        list of list of str
        A list containing three sublists, each with one element: the name of a top developer for the given year.
        """
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
async def best_developer_year(developer: str):
    def read_parquets ():
        """
        This function reads a parquet file from a local directory and returns a pandas dataframe.

        Returns
        -------
        pandas.DataFrame
        A dataframe containing the data from the parquet file api_query5_dev_sent.parquet, which has columns: developer, Positive, and Negative with the sentiment scores of the reviews for each developer.
        """
        dev_sentiment_df = pd.read_parquet('.\\Data\\API\\api_query5_dev_sent.parquet')
        return dev_sentiment_df
    
    def retrieve_dev_sent(dev_sentiment_df,developer):
        """
        This function queries a dataframe of developer sentiment scores and returns the positive and negative scores for a given developer.

        Parameters
        ----------
        dev_sentiment_df : pandas.DataFrame
        The dataframe of developer sentiment scores with columns: developer, Positive, and Negative.
        developer : str
        The name of the developer to query the dataframe by.

        Returns
        -------
        tuple of (float, float)
        A tuple containing the positive and negative scores for the given developer.
        """
        dev_data = dev_sentiment_df.query("developer == @developer")
        positive = (dev_data['Positive'])
        negative = (dev_data['Negative'])
        positive_value= positive.iloc[0]
        negative_value= negative.iloc[0]
        return positive_value,negative_value

    dev_sentiment_df = read_parquets()

    if developer not in dev_sentiment_df.index:
        return {"message": "Invalid developer name. Please try again."}

    positive_value,negative_value = retrieve_dev_sent(dev_sentiment_df, developer)
    positive_value = int(positive_value)
    negative_value = int(negative_value)
    json_response = {developer: {"Positive": positive_value, "Negative": negative_value}}

    return json_response

@app.get("/game_recommendation/{game_id}")
async def game_recommendation(game_id: int):
    def read_parquets ():
            ml1_dataframe = pd.read_parquet('ml1_games_for_model.parquet')
            return ml1_dataframe
    
    def use_get_similar_games(game_id,dataframe):
        result = get_similar_games(game_id,dataframe)
        return result

    dataframe = read_parquets ()

    response = use_get_similar_games(game_id,dataframe)
    print(response)
    return {"Juegos Recomendados:" : response}