# 12_HandleTradeData

## Problem

How to review your trades so that you can see multiple timeframes at the same time. Understanding price position is an important part of crafting your edge in the markets

## Solution

Build database to organize trading data and executions. This will make pattern recognition much easier in the future similar situations.


## Database design

There are 3 different tables for each market data timeframe. TradeId is used as primary key between these. All trades and their details are saved into trades table. Executions are fetched from Interactive Brokers .tlg tradelog file when processing the data.


<img width="829" height="735" alt="image" src="https://github.com/user-attachments/assets/b92618ac-5d48-4a8d-9c04-777731511521" />


<img width="661" height="238" alt="image" src="https://github.com/user-attachments/assets/6f8b4316-1a79-4cfc-8956-cb74b8ae0fbb" />


Example query from table marketdatad
<img width="1337" height="623" alt="image" src="https://github.com/user-attachments/assets/20e4e7be-541f-4636-a947-05b24f62ac6e" />


# Software components

## Common Folder

The common folder contains pieces of software that are commonly used across multiple projects. My aim is to keep this folder up-to-date so that calculations, such as the VWAP example, are always executed using the same code, ensuring consistency.

## Helpers Folder

The helpers folder contains assisting functions. This folder still constitutes a major part of the program logic. For example:

HandleExecutions.py – Processes execution data from .tlg files.

ReadTlgFile.py – Reads Interactive Brokers trade log files.

FetchIBData.py – Handles fetching data from Interactive Brokers.

HandleDataFrames.py – Manages incoming bar data, which is already provided in a Pandas DataFrame structure.

## Configuration

config.json – Specifies paths for .tlg files and, potentially, locations for manual data entry. Also TWS API connection details are here.

## Main Entry Point

Main.py – The main script where the program starts execution.

## Database folder

DBfunctions.py is responsible for all my database insert and fetch operations. Database is being build on top of PostgresSQL. As service providor I use Heroku
