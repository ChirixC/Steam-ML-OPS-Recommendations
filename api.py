from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pyarrow as pa
import pyarrow.parquet as pq
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
