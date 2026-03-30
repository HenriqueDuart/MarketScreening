from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()
API_CODE=os.getenv('API_TOKEN')

## EODHD data filters
# Filter to the first query (basic check, not heavy)
FILTER_CHECK='General::Type,General::Name,General::IsDelisted'
# Filter for static data (e.g. Name, ISIN, industry, etc.)
FILTER_STATIC = (
    "General::Code,General::Type,General::Name,General::Exchange,"
    "General::CurrencyCode,General::CountryName,General::ISIN,"
    "General::FiscalYearEnd,General::Sector,General::Industry,"
    "General::GicSector,General::GicGroup,General::GicIndustry,"
    "General::GicSubIndustry,General::IsDelisted,General::Description,"
    "General::WebURL,"
    "Highlights,Valuation,SharesStats,Technicals,AnalystRatings"
)
# Filter for time series (e.g. quarterly financial data, balance sheet, earnings, etc.)
FILTER_TIMESERIES = (
    "Earnings::History,"
    "Financials::Balance_Sheet::currency_symbol,"
    "Financials::Balance_Sheet::quarterly,"
    "Financials::Cash_Flow::currency_symbol,"
    "Financials::Cash_Flow::quarterly,"
    "Financials::Income_Statement::currency_symbol,"
    "Financials::Income_Statement::quarterly"
)
# Countries that will be covered in the extract
selected_country_list=['Portugal']

with open('Outputs/Tickers/World_tickers.json', mode='r', encoding='utf-8') as file:
    # Loading the list of all available tickers per country
    country_tickers = json.load(file)

def extract_security_data(country_list,data_filter, country_tickers_dict):
    results = {}
    for country in country_list:
        # Adding country entry to the final dictionary
        if country not in results:
            results[country] = {}
        print(f'Searching {country} venues.')
        country_venues = country_tickers_dict.get(country, {})
        if not country_venues:
            print(f"No venues found for {country}")
            continue
        for venue, tickers in country_venues.items():
            country_venue_key = f"{country}.{venue}"
            if country_venue_key not in results[country]:
                results[country][country_venue_key] = {}
            print(f'Venue found. Searching tickers for venue: {venue}')
            for ticker in tickers:
                security_code = f'{ticker}.{venue}'
                url = f'https://eodhd.com/api/fundamentals/{security_code}'
                parameters = {
                    'api_token': API_CODE,
                    'fmt': 'json',
                    'filter':FILTER_CHECK
                }
                # Retrieving 3 data points only: security type, name and listed/delisted flag
                try:
                    response = requests.get(url=url, params=parameters, timeout=30)
                    response.raise_for_status()
                    check_data = response.json()

                except Exception as e:
                    with open('./Errors/filter_getrq.txt', mode='a', encoding='utf-8') as file:
                        file.write(f"{security_code} | {e}\n")
                    print(f"Failed on filtering for {security_code}: {e}")
                    continue

                if not isinstance(check_data, dict):
                    with open("./Errors/filter_structure.txt", mode="a", encoding="utf-8") as file:
                        file.write(f"{security_code} | non-dict response: {repr(check_data)}\n")
                    print(f"Skipping: {security_code} --- non-dict response: {repr(check_data)}")
                    continue

                security_type = check_data.get("General::Type")
                is_delisted = check_data.get("General::IsDelisted")
                security_name = check_data.get("General::Name", "")

                # Checking if it is a currently listed Common Stock
                if security_type=='Common Stock' and is_delisted is not True:
                    final_parameters = {
                        'api_token': API_CODE,
                        'fmt': 'json',
                        'filter': data_filter
                    }
                    print(f"Retrieving data for the following security: {security_code} --- {security_name} ")
                    # If Common stock and currently listed then run the real query
                    try:
                        response = requests.get(url=url, params=final_parameters, timeout=30)
                        response.raise_for_status()
                        security_data = response.json()

                        # Append to the existing list
                        results[country][country_venue_key][security_code] = security_data

                    except Exception as e:
                        with open('./Errors/security_getrq.txt', mode='a', encoding='utf-8') as file:
                            file.write(f"{security_code} | {e}\n")
                        print(f"Failed on retrieving data for {security_code}: {e}")
                        continue

                else:
                    print(f"Skipping: {security_code} --- {security_name}. Security type: {security_type}, Is listed: {is_delisted}\nSecurity was not Common Stock or is not currently listed.")
    return results

# Running the function for the static query
raw_data_static = extract_security_data(selected_country_list,FILTER_STATIC,country_tickers)
print(f"Total securities retrieved: {len(raw_data_static)}")

# Running the function for the dynamic query
raw_data_dynamic = extract_security_data(selected_country_list,FILTER_TIMESERIES,country_tickers)
print(f"Total securities retrieved: {len(raw_data_dynamic)}")

# Printing the final data
os.makedirs("Outputs", exist_ok=True)
with open("./Outputs/Raw_data/raw_data_static.json", "w", encoding="utf-8") as file:
    json.dump(raw_data_static, file, indent=4, ensure_ascii=False)

with open("./Outputs/Raw_data/raw_data_dynamic.json", "w", encoding="utf-8") as file:
    json.dump(raw_data_dynamic, file, indent=4, ensure_ascii=False)