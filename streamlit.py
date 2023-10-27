import streamlit as st
import requests

# Define the FastAPI endpoint
FASTAPI_URL = "http://0.0.0.0:10000"

# Create a Streamlit app
st.title("César Chirino ML Ops")

user_id = st.text_input("User Id")
button = st.button("Click me")
st.text('Response')

response = st.empty()

if button:
    params = {"user_id": str(user_id)}
    r = requests.get(FASTAPI_URL + "/user", params=params)

    if r.status_code == 200:
        print(r)
        print(type(r))
        result = r.json().get("result")
        response.success(f'The result is {result}')
    else:
        response.error('Something went wrong')
