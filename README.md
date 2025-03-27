# Sentiment Analysis on IMDB Reviews using Azure 

Inline-style: 
![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png "Logo Title Text 1")

## Project Overview
This project performs **Sentiment Analysis** on IMDB movie reviews using **Azure SQL Database**, **Python (NLTK, Pandas, SQLAlchemy)**, and **Azure Data Factory (ADF)** for data ingestion. The results are then stored in **Azure SQL** for visualization in **Power BI or Tableau**.

## Project Workflow
1. **Data Ingestion**: Import IMDB review data into **Azure SQL Database** using **Azure Data Factory (ADF)**.
2. **Sentiment Analysis**:
   - Use **NLTK's SentimentIntensityAnalyzer (VADER)** to classify reviews as **Positive, Negative, or Neutral**.
   - Store the processed results back into **Azure SQL Database**.
3. **Data Visualization**: Connect **Power BI or Tableau** to **Azure SQL** to analyze sentiment trends.

## Technologies Used
- **Azure SQL Database** (Cloud storage)
- **Azure Data Factory (ADF)** (Data ingestion)
- **Python** (Processing & Sentiment Analysis)
- **NLTK (VADER)** (Natural Language Processing)
- **SQLAlchemy & pyodbc** (Database connectivity)
- **Power BI / Tableau** (Visualization)



