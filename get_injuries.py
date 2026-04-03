# PC SIDE SCRIPT
import requests
import pandas as pd
from bs4 import BeautifulSoup

url = "https://www.cbssports.com/nba/injuries/"
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
soup = BeautifulSoup(r.text, 'html.parser')

injured_players = [a.text.strip() for a in soup.select('span.CellPlayerName--long a')]

pd.Series(injured_players, name="Player").to_csv("injuries.csv", index=False)