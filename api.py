from fastapi import FastAPI
import pyarrow as pa
import pyarrow.parquet as pq
import os

app = FastAPI()
port = os.environ.get("PORT", 8000) # 
app.run(host="0.0.0.0", port=port) # 

clients = pq.read_table('user_items_df.parquet')
clients_pd = pa.Table.to_pandas(clients)

def get_items_amount(user):
    # Debería checkar si esta lista tiene duplicados
    items = clients_pd.loc[clients_pd['user_id'] == user, 'items']  
    items2 = items[0]
    return items2.size

@app.get('/user/')
def get_client_items(user_id: str):
    count = get_items_amount(user_id)
    return {"result": count}

@app.get('/clients/{user}')
def get_client_items(user):
    count = get_items_amount(user)
    return count

@app.get("/")
def index():
    return {"message": "helloo 2"}
