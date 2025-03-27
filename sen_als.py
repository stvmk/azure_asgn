import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sqlalchemy import create_engine, text
import urllib

server = 'tcp:imdbdataserver.database.windows.net,1433'
database = ''  
username = ''
password = ''  
driver = 'ODBC Driver 18 for SQL Server'

conn_str = urllib.parse.quote_plus(
    f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

# Create SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

try:
    with engine.connect() as connection:
        df = pd.read_sql(text("SELECT * FROM dbo.imdb_reviews"), connection)  
    print("Data loaded successfully!")
except Exception as e:
    print(f"Failed to load data: {e}")
    exit()

nltk.download('vader_lexicon', quiet=True)
sia = SentimentIntensityAnalyzer()

def analyze_sentiment(review):
    """Analyzes sentiment using VADER and returns Positive, Negative, or Neutral."""
    if not isinstance(review, str):  # Handle non-string inputs
        return "Neutral"
    score = sia.polarity_scores(review)['compound']
    return "Positive" if score >= 0.05 else "Negative" if score <= -0.05 else "Neutral"

df['Sentiment'] = df['review'].apply(analyze_sentiment)

batch_size = 500 

try:
    df.to_sql(
        "IMDB_Sentiment_Results",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=batch_size  
    )
    print("Sentiment analysis completed and results saved to Azure SQL!")
except Exception as e:
    print(f"Failed to save results: {e}")
