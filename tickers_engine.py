from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()
DICT_LOC='./Exchanges/Country_dict.json'
ALL_TICKERS_LOC='./Tickers/All_tickers.json'


# Load the existing dictionary with all venues
with open(DICT_LOC, mode="r", encoding="utf-8") as file:
    country_dict = json.load(file)


def all_tickers_dict_generator(input_dict):
    parameters={
        'api_token':os.getenv('API_TOKEN'),
        'fmt':'json'
    }

    tickers_dict={}
    for country, country_details in input_dict.items():
        for venue in country_details['venue_codes']:
            print(f'Getting tickers for {country}, venue: {venue}.')
            tickers_endpoint = f'https://eodhd.com/api/exchange-symbol-list/{venue}'
            response = requests.get(url=tickers_endpoint, params=parameters)
            response.raise_for_status()
            data = response.json()
            tickers_list =[item['Code'] for item in data if 'Code' in item]

            if country not in tickers_dict:
                tickers_dict[country]={venue:tickers_list}
            else:
                tickers_dict[country][venue]=tickers_list

    print('Retrieved all tickers for all venues')
    with open(ALL_TICKERS_LOC, mode='w', encoding="utf-8") as file:
        json.dump(tickers_dict, file, indent=4)
        print('File saved.')

    return tickers_dict

final_dict = all_tickers_dict_generator(country_dict)


