import streamlit as st 
import requests as rq
import time

def fire_requests():
    r = rq.get('https://stocksly.onrender.com/stocks/get_available_stocks/')
    stocks = r.content 
    time.sleep(10)
    
st.write("this app powers https://stocksly.onrender.com/ ")
st.write("created by https://github.com/AYUSHKHAIRE/")
while True:
    fire_requests()