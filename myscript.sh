mv groupings.csv groupings_old.csv
python3 ConcertScraper.py 
python3 ConcertDataProcessor.py
python3 delta.py
