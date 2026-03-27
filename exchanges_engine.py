from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()

# Variables
API_TOKEN = os.getenv('API_TOKEN')
EXCHANGE_FILE = "./Exchanges/Exchanges.json"
COUNTRY_DICT = './Exchanges/Country_dict.json'

def get_exchanges():
    """ Retrieves all available exchanges venues in EODHD, and their respective codes. It returns a list, and saves the same information in a .txt file. """
    exchanges_endpoint = f'https://eodhd.com/api/exchanges-list/?api_token={API_TOKEN}&fmt=json'
    trading_venue_list = requests.get(url=exchanges_endpoint).json()


    with open(EXCHANGE_FILE, mode="w", encoding="utf-8") as file:
        print(f'Exchange list retrieved. Saving information into {EXCHANGE_FILE} file...')
        json.dump(trading_venue_list, file, indent=4)
        print('File saved.')

    return trading_venue_list

def transform_exchange_list(input_list):
    """ Create a new dict, in the desired format. Dict is organized per country, each subdict containing lists of different elements, e.g. exchange codes, etc."""

    country_dict = {}

    # Create / populate the new dictionary
    print('Creating new dictionary. Formatting...')
    for item in input_list:
        if item['Country'] not in country_dict:
            country_dict[item['Country']]={
                'venue_codes':[item['Code']],
                'operatingMIC':[item['OperatingMIC']],
                'exchange_names':[item['Name']],
                'exchange_currencies':[item['Currency']]
            }
        else:
            country_dict[item['Country']]['venue_codes'].append(item['Code'])
            country_dict[item['Country']]['operatingMIC'].append(item['OperatingMIC'])
            country_dict[item['Country']]['exchange_names'].append(item['Name'])
            country_dict[item['Country']]['exchange_currencies'].append(item['Currency'])

    # Removing duplicates for the exchange currencies
    print('Dictionary created. Removing duplicates...')
    for country, items in country_dict.items():
        currencies = list(dict.fromkeys(items['exchange_currencies']))
        country_dict[country]['exchange_currencies'] = currencies[0] if len(currencies) == 1 else currencies

    print('Saving file.')
    with open(COUNTRY_DICT, mode='w', encoding="utf-8") as file:
        json.dump(country_dict, file, indent=4)
        print('File saved.')

    return country_dict

exchange_list = get_exchanges()
exchange_dict = transform_exchange_list(exchange_list)

print('The program run successfully.')

