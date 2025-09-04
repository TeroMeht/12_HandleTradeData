# 12_HandleTradeData

## Problem

How to review your trades so that you can see multiple timeframes at the same time. Understanding price position is an important part of crafting your edge in the markets

## Solution

Trade database for multiple timeframes. The software is divided into 2 separate sections. One is taking care of database and it's operations. Other one is responsble for UI operations where user can review and categories trades.
Executions are shown on 2 minute chart


## Database design

There are 3 different tables for each market data timeframe. TradeId is used as primary key between these. All trades and their details are saved into trades table. Executions are fetched from Interactive Brokers .tlg tradelog file when processing the data.

<img width="633" height="563" alt="image" src="https://github.com/user-attachments/assets/02fc3d1d-88e3-4c66-bbc2-51bb3b61a655" />



## UI Trade Review

Run_PlotTradeData.py starts Trade viewer tool where user can dive deepering into ones trades and executions. There are functionalities to categories and rate trades. These changes are saved into the database.

<img width="1259" height="671" alt="image" src="https://github.com/user-attachments/assets/a0d3bd7e-0721-43f8-bbee-677c058672c9" />
