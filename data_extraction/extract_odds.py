from bs4 import BeautifulSoup, SoupStrainer
import requests
import pandas as pd
import chardet
from tqdm import tqdm

page = requests.get("http://www.tennis-data.co.uk/alldata.php")
out_df=pd.DataFrame([],columns=['Date','Winner','Loser'])
soup = BeautifulSoup(page.content, 'html.parser')
links=[link["href"] for link in soup.select('a[href*=".xls"]') if "w" not in link["href"] and int(link['href'][:4])>2001]
for link in tqdm(links):
    df=pd.read_excel('http://www.tennis-data.co.uk/'+link)
    out_df=out_df.append(df[['Date','Winner','Loser','B365W','B365L']])
out_df['Date'] = pd.to_datetime(out_df['Date'])
out_df=out_df.dropna()
print(out_df.sort_values(by="Date"))