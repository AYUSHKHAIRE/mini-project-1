import pandas as pd
from xgboost import XGBRegressor
import streamlit as st
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Load and prepare data
df = pd.read_csv('prepareforsttack.csv')
ndf = df.drop(columns=['Unnamed: 0', 'Prediction Date'])

highdf = ndf[['Best Predicted High', 'Mean Predicted High', 'Actual High', 'dnn_lstm_high', 'cnn_lstm_high', 'gnn_lstm_high']]
lowdf = ndf[['Best Predicted Low', 'Mean Predicted Low', 'Actual Low', 'dnn_lstm_low', 'cnn_lstm_low', 'gru_lstm_low']]

total_dates = list(df['Prediction Date'].tail(30))

# Function to train and predict
def train_and_predict_xgb(index):
    X_high = highdf.drop(columns=['Actual High'])
    y_high = highdf['Actual High']
    
    X_low = lowdf.drop(columns=['Actual Low'])
    y_low = lowdf['Actual Low']
    
    X_train_high, X_test_high = X_high.iloc[:index], X_high.iloc[index:]
    y_train_high, y_test_high = y_high.iloc[:index], y_high.iloc[index:]
    
    X_train_low, X_test_low = X_low.iloc[:index], X_low.iloc[index:]
    y_train_low, y_test_low = y_low.iloc[:index], y_low.iloc[index:]

    hxgb = XGBRegressor(
        learning_rate=0.3,
        max_depth=6,
        n_estimators=50,
        subsample=0.8,
        colsample_bytree=1.0,
        gamma=0.2,
        random_state=42
    )
    hxgb.fit(X_train_high, y_train_high)
    
    lxgb = XGBRegressor(
        learning_rate=0.3,
        max_depth=6,
        n_estimators=50,
        subsample=0.8,
        colsample_bytree=1.0,
        gamma=0.2,
        random_state=42
    )
    lxgb.fit(X_train_low, y_train_low)

    high_pred = hxgb.predict(X_test_high)
    low_pred = lxgb.predict(X_test_low)
    
    return high_pred, low_pred , hxgb , lxgb

def get_stocks_daily_data_chart(stocksymbol='TSLA', startdate=total_dates[0], enddate=total_dates[-1]):
    r = requests.get(f"https://stocksly.onrender.com/stocks/get_stock_daily_data/{stocksymbol}/?start={startdate}&end={enddate}")
    if r.status_code == 200:
        data = r.json()['data']
        time = data['time']
        close = data['close']
        open_ = data['open']
        low = data['low']
        high = data['high']
        volume = data['volume']
        
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True, 
            row_heights=[0.7, 0.3], 
            vertical_spacing=0.03
        )

        fig.add_trace(
            go.Candlestick(
                x=time,
                open=open_,
                high=high,
                low=low,
                close=close,
                name="Price"
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Bar(
                x=time, 
                y=volume, 
                name="Volume",
                marker_color='blue',
                opacity=0.6
            ),
            row=2, col=1
        )

        fig.update_layout(
            template='plotly_dark',
            title=f"Daily Stock Data for {stocksymbol}",
            xaxis_title="Date",
            yaxis_title="Price",
            yaxis2_title="Volume",
            showlegend=False,
            xaxis_rangeslider_visible=False
        )

        return fig
    else:
        st.error(f"Failed to fetch data for {stocksymbol}. Status code: {r.status_code}")
        return None

st.header("Stock Price Prediction for TSLA")
selected_date = st.selectbox("Select a prediction date:", index=len(total_dates) - 1,options = total_dates)
index = total_dates.index(selected_date) - len(total_dates)  # Convert to relative index
high_pred, low_pred , hxgb , lxgb= train_and_predict_xgb(index)

st.write(f"Predictions for {selected_date}:")
st.write(f"Predicted High: {high_pred[0]:.2f}")
st.write(f"Predicted Low: {low_pred[0]:.2f}")

resultsdf = df.copy()

actual_highs = resultsdf['Actual High']
actual_lows = resultsdf['Actual Low']

X_high = highdf.drop(columns=['Actual High'])  
X_low = lowdf.drop(columns=['Actual Low'])  

hxgb_high = hxgb.predict(X_high)
lxgb_low = lxgb.predict(X_low)

resultsdf['xgb high'] = hxgb_high
resultsdf['xgb low'] = lxgb_low

dnn_lstm_low = resultsdf['dnn_lstm_low']
cnn_lstm_low = resultsdf['cnn_lstm_low']
gru_lstm_low = resultsdf['gru_lstm_low']
dnn_lstm_high = resultsdf['dnn_lstm_high']
cnn_lstm_high = resultsdf['cnn_lstm_high']
gru_lstm_high = resultsdf['gnn_lstm_high']
xgb_high = resultsdf['xgb high']
xgb_low = resultsdf['xgb low']
mean_predicted_highs = resultsdf['Mean Predicted High']
mean_predicted_lows = resultsdf['Mean Predicted Low']
weighted_predicted_highs = resultsdf['Best Predicted High']
weighted_predicted_lows = resultsdf['Best Predicted Low']

stock_symbol = 'TSLA'
fig = get_stocks_daily_data_chart(stock_symbol, total_dates[0], selected_date)
if fig:
    st.plotly_chart(fig)

resultsdf['Prediction Date'] = pd.to_datetime(resultsdf['Prediction Date'])

fig = go.Figure()
resultsdf = resultsdf.tail((20))

# DNN LSTM Model
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['dnn_lstm_high'],
                         mode='lines+markers', name='DNN LSTM Predicted High', 
                         line=dict(color='red', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['dnn_lstm_low'],
                         mode='lines+markers', name='DNN LSTM Predicted Low', 
                         line=dict(color='red', dash='dot'),
                         ))

# CNN LSTM Model
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['cnn_lstm_high'],
                         mode='lines+markers', name='CNN LSTM Predicted High', 
                         line=dict(color='blue', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['cnn_lstm_low'],
                         mode='lines+markers', name='CNN LSTM Predicted Low', 
                         line=dict(color='blue', dash='dot'),
                         ))

# GRU LSTM Model
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['gnn_lstm_high'],
                         mode='lines+markers', name='GRU LSTM Predicted High', 
                         line=dict(color='green', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['gru_lstm_low'],
                         mode='lines+markers', name='GRU LSTM Predicted Low', 
                         line=dict(color='green', dash='dot'),
                         ))

# Mean Model
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Mean Predicted High'],
                         mode='lines+markers', name='Mean Predicted High', 
                         line=dict(color='cyan', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Mean Predicted Low'],
                         mode='lines+markers', name='Mean Predicted Low', 
                         line=dict(color='cyan', dash='dot'),
                         ))

# Weighted Model
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Best Predicted High'],
                         mode='lines+markers', name='Weighted Predicted High', 
                         line=dict(color='magenta', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Best Predicted Low'],
                         mode='lines+markers', name='Weighted Predicted Low', 
                         line=dict(color='magenta', dash='dot'),
                         ))

# Actual High and Low
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Actual High'],
                         mode='lines+markers', name='Actual High', 
                         line=dict(color='orange', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['Actual Low'],
                         mode='lines+markers', name='Actual Low', 
                         line=dict(color='orange', dash='dot'),
                         ))

# xgb High and Low
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['xgb high'],
                         mode='lines+markers', name='XGB High', 
                         line=dict(color='white', dash='dash'),
                         ))
fig.add_trace(go.Scatter(x=resultsdf['Prediction Date'], y=resultsdf['xgb low'],
                         mode='lines+markers', name='XGB Low', 
                         line=dict(color='white', dash='dot'),
                         ))

fig.update_layout(
    title=f"Stock Price Prediction (All Models) for TSLA",
    xaxis_title="Date",
    yaxis_title="Price",
    template='plotly_dark',
    xaxis=dict(tickangle=-45),
    height=700,  
)

st.plotly_chart(fig)