import os
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sqlalchemy import create_engine, text
import urllib
from sklearn.feature_extraction.text import CountVectorizer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from environment variables
server = os.getenv('DB_SERVER', 'tcp:imdbdataserver.database.windows.net,1433')
database = os.getenv('DB_DATABASE', 'imdb_server')
username = os.getenv('DB_USERNAME', 'admin_root')
password = os.getenv('DB_PASSWORD', 'Stemx0a$')  # Replace with environment variable in production
driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')

# Properly format the connection string
conn_str = urllib.parse.quote_plus(
    f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

# Create SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

def load_data_from_sql():
    """Loads data from the SQL database."""
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text("SELECT * FROM dbo.imdb_reviews"), connection)
        logging.info("Data loaded successfully!")
        return df
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise

def perform_sentiment_analysis(df):
    """Performs sentiment analysis on the reviews."""
    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()

    def analyze_sentiment(review):
        """Analyzes sentiment using VADER and returns Positive, Negative, or Neutral."""
        if not isinstance(review, str):  # Handle non-string inputs
            return "Neutral"
        score = sia.polarity_scores(review)['compound']
        return "Positive" if score >= 0.05 else "Negative" if score <= -0.05 else "Neutral"

    if 'review' not in df.columns:
        logging.error("'review' column is missing in the dataset.")
        raise ValueError("The dataset must contain a 'review' column.")

    df['Sentiment'] = df['review'].apply(analyze_sentiment)
    logging.info("Sentiment analysis completed.")
    return df

def save_to_sql(df, table_name, chunksize=500):
    """Saves a DataFrame to the SQL database."""
    try:
        df.to_sql(
            table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=chunksize
        )
        logging.info(f"Data saved to table '{table_name}' in Azure SQL.")
    except Exception as e:
        logging.error(f"Failed to save data to table '{table_name}': {e}")
        raise

def calculate_word_frequencies(reviews, ngram_range=(1, 1), top_n=10):
    """Calculates word or phrase frequencies."""
    vectorizer = CountVectorizer(ngram_range=ngram_range, stop_words='english')
    word_matrix = vectorizer.fit_transform(reviews.dropna())  # Drop NaN reviews
    word_counts = word_matrix.sum(axis=0).A1
    vocab = vectorizer.get_feature_names_out()
    word_freq = sorted(zip(vocab, word_counts), key=lambda x: x[1], reverse=True)
    return word_freq[:top_n]

def analyze_word_and_phrase_frequencies(df):
    """Analyzes and saves word and phrase frequencies."""
    top_words = calculate_word_frequencies(df['review'], ngram_range=(1, 1), top_n=10)
    top_phrases = calculate_word_frequencies(df['review'], ngram_range=(2, 2), top_n=10)

    logging.info(f"Top 10 Words: {top_words}")
    logging.info(f"Top 10 Phrases: {top_phrases}")

    # Save to SQL
    save_to_sql(pd.DataFrame(top_words, columns=['Word', 'Frequency']), "Top_Words")
    save_to_sql(pd.DataFrame(top_phrases, columns=['Phrase', 'Frequency']), "Top_Phrases")

def analyze_sentiment_by_score(df):
    """Analyzes sentiment distribution by score and saves to SQL."""
    if 'score' not in df.columns:
        logging.warning("No 'score' column found in the dataset.")
        return

    sentiment_by_score = df.groupby('score')['Sentiment'].value_counts(normalize=True).unstack()
    logging.info("Sentiment Distribution by Score:")
    logging.info(f"\n{sentiment_by_score}")

    # Save to SQL
    save_to_sql(sentiment_by_score.reset_index(), "Sentiment_By_Score")

def main():
    """Main function to orchestrate the analysis."""
    try:
        # Step 1: Load data
        df = load_data_from_sql()

        # Step 2: Perform sentiment analysis
        df = perform_sentiment_analysis(df)
        save_to_sql(df, "IMDB_Sentiment_Results")

        # Step 3: Analyze word and phrase frequencies
        analyze_word_and_phrase_frequencies(df)

        # Step 4: Analyze sentiment by score
        analyze_sentiment_by_score(df)

        logging.info("All tasks completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()