'''
Title: MLB The Show Market History
Purpose: This script will download your completed transactions from theshownation.com
         It will also load your transactions into into a PostgreSQL database if needed.
         You can uncomment the function and variables in __main__ that writes results to your PostgreSQL instance if necessary
'''

from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd
from sqlalchemy import create_engine
requests.packages.urllib3.disable_warnings()


def process_market_history(csv_file, pages, browser_headers):
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csv_file:
            market_file = csv.DictWriter(csv_file, fieldnames=['name', 'purchase_type', 'price', 'time'])
            market_file.writeheader()

            for x in range(1, pages):
                print(f'Processing page {x}')

                r = requests.get(f'https://mlb21.theshow.com/orders/completed_orders?page={x}&', headers=browser_headers)
                # print(r.url)
                pagetext = r.text
                soup = BeautifulSoup(pagetext, 'html.parser')
                # print(soup)

                table = soup.find('tbody')
                index = table.find_all('tr')

                for i in index:
                    name = i.contents[1].text.strip()
                    purchase_type = i.contents[3].text.split(' ')[0].strip()
                    amount_i = i.contents[3].text.split(' ')[1].strip()
                    amount = amount_i[3:].strip()
                    time = i.contents[5].text
                    print(f'{purchase_type} {name} for {amount} @ {time}')
                    market_file.writerow({'name': name, 'purchase_type': purchase_type, 'price': amount, 'time': time})
    except Exception as e:
        print(e)


def process_sql(db_connection, db_table, csv_file):
    try:

        engine = create_engine(db_connection)

        transactions = pd.read_csv(csv_file)
        transactions.columns = map(str.lower, transactions.columns)
        transactions = transactions.replace('\n', '', regex=True)

        transactions['price'] = transactions['price'].str.replace(',', '').astype(int)
        transactions["price"] = pd.to_numeric(transactions["price"])

        transactions['date'] = pd.to_datetime(transactions['time'])
        transactions['date'] = transactions['date'].dt.strftime('%m-%d-%Y')

        transactions.to_sql(db_table, con=engine, index=False, if_exists='replace')

    except Exception as error:
        print(error)


if __name__ == '__main__':

    # These headers need to be updated with from your browser
    headers = {
    'authority': 'mlb21.theshow.com',
    'sec-ch-ua': '^\\^',
    'accept': 'text/html, application/xhtml+xml',
    'turbolinks-referrer': 'https://mlb21.theshow.com/orders/completed_orders?page=2&^#',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://mlb21.theshow.com/orders/completed_orders?page=2&',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'ab.storage.userId.bbce52ad-c4ca-45bc-9c03-b1183aff5ee5=^%^7B^%^22g^%^22^%^3A^%^226166541^%^22^%^2C^%^22c^%^22^%^3A1619985207667^%^2C^%^22l^%^22^%^3A1619985207667^%^7D; ab.storage.deviceId.bbce52ad-c4ca-45bc-9c03-b1183aff5ee5=^%^7B^%^22g^%^22^%^3A^%^221348a36a-49d1-d499-ef68-112b5d354317^%^22^%^2C^%^22c^%^22^%^3A1619985207680^%^2C^%^22l^%^22^%^3A1619985207680^%^7D; tsn_hide_ribbon=1-1619813294; _ga_EJKYYHZPBF=GS1.1.1620063258.5.1.1620063280.0; _ga=GA1.2.1077199731.1619797198; tsn_token=eyJhbGciOiJIUzI1NiJ9.eyJpZCI6MzM5NTkwLCJ1c2VybmFtZSI6Im1penp0aWdlcnMxX1hCTCIsInBpY3R1cmUiOiJodHRwczovL3RoZXNob3duYXRpb24tcHJvZHVjdGlvbi5zMy5hbWF6b25hd3MuY29tL2ZvcnVtX2ljb25zL2ljb25fc3RsX2Fic3RyYWN0LnBuZyIsImdyb3VwcyI6W119.R5AT85paUj7zXW-lXBBd7snhqB2ijaYNxtOwuKq38EM; _gid=GA1.2.102046337.1622911597; tsn_last_seen_roster_id=3; tsn_last_url=--CCFFYHBDZE06TDQk68hDkqHLuA40Yxf2AOzE1V9IHa7QVOCRyt63-4OfPSUEP1vVILszMFqnlYUd-CwJMsXA^%^3D^%^3D; _gat_gtag_UA_13296316_21=1; _tsn_session=ed5976901ab04225eb5777d714d0e1ce; ab.storage.sessionId.bbce52ad-c4ca-45bc-9c03-b1183aff5ee5=^%^7B^%^22g^%^22^%^3A^%^225badb6f3-ac67-f62a-5790-f3405b9bd740^%^22^%^2C^%^22e^%^22^%^3A1622920434345^%^2C^%^22c^%^22^%^3A1622918634345^%^2C^%^22l^%^22^%^3A1622918634345^%^7D',
}


    # Set the following variables
    market_csv = 'market_history.csv'
    num_pages = 1000
    #postgres_conn = 'postgresql+psycopg2://postgres:postgres@localhost:5433/show?gssencmode=disable'
    #db_table = 'transactions'

    # Go!
    print('processing market history...')
    process_market_history(market_csv, num_pages, headers)

    #print('processing sql table(s)...')
    #process_sql(postgres_conn, db_table, market_csv)
