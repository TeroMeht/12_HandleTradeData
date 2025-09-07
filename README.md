# 12_HandleTradeData

## Problem

How to review your trades so that you can see multiple timeframes at the same time. Understanding price position is an important part of crafting your edge in the markets

## Solution

Build database in order to organize trading data and executions. This will make pattern recognition much easier in the future similar situations.


## Database design

There are 3 different tables for each market data timeframe. TradeId is used as primary key between these. All trades and their details are saved into trades table. Executions are fetched from Interactive Brokers .tlg tradelog file when processing the data.

<img width="633" height="563" alt="image" src="https://github.com/user-attachments/assets/02fc3d1d-88e3-4c66-bbc2-51bb3b61a655" />



<img width="661" height="238" alt="image" src="https://github.com/user-attachments/assets/6f8b4316-1a79-4cfc-8956-cb74b8ae0fbb" />


Example query from table marketdatad
<img width="1337" height="623" alt="image" src="https://github.com/user-attachments/assets/20e4e7be-541f-4636-a947-05b24f62ac6e" />
