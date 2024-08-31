import asyncio
import aiohttp
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)


async def fetch_historical_forex_data(session, ticker, multiplier, timespan, from_date, to_date, api_key):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}?apiKey={api_key}"
    async with session.get(url) as response:
        if response.status != 200:
            logging.error(f"Failed to fetch data: {response.status} - {await response.text()}")
            return None
        data = await response.json()
        return data


async def process_data(data):
    if data is None or 'results' not in data:
        logging.error("No results in data response")
        return None
    results = data['results']
    if not results:
        logging.error("No data points in results")
        return None
    df = pd.DataFrame(results)
    df['t'] = pd.to_datetime(df['t'], unit='ms')
    df.set_index('t', inplace=True)
    return df


async def fetch_data_in_batches(session, ticker, multiplier, timespan, start_date, end_date, api_key):
    # Initialize an empty list to store the dataframes
    dfs = []

    # Convert dates to datetime objects
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # Initialize batch start date
    batch_start = start

    while batch_start < end:
        # Calculate batch end date
        batch_end = min(batch_start + pd.Timedelta(minutes=5000 * multiplier), end)

        # Fetch the data for this batch
        logging.info(f"Fetching data from {batch_start.strftime('%Y-%m-%d')} to {batch_end.strftime('%Y-%m-%d')}")
        data = await fetch_historical_forex_data(session, ticker, multiplier, timespan,
                                                 batch_start.strftime('%Y-%m-%d'), batch_end.strftime('%Y-%m-%d'),
                                                 api_key)
        df = await process_data(data)
        if df is not None:
            dfs.append(df)
        else:
            logging.error(
                f"No data fetched for batch from {batch_start.strftime('%Y-%m-%d')} to {batch_end.strftime('%Y-%m-%d')}")

        # Update batch start date for the next iteration
        batch_start = batch_end

        # Introduce a delay to avoid hitting the rate limit
        await asyncio.sleep(12)  # Assuming 5 requests per minute, this gives a buffer

    # Check if any data was fetched
    if not dfs:
        logging.error("No data fetched in any batch")
        return None

    # Concatenate the dataframes
    combined_df = pd.concat(dfs, ignore_index=False)
    return combined_df


async def main():
    ticker = "C:EURUSD"  # EUR/USD forex pair
    multiplier = 1  # Multiplier for the timespan (1 minute in this case)
    timespan = "minute"  # Timespan (minute, hour, day, etc.)
    start_date = "2024-08-01"  # Start date
    end_date = "2024-08-31"  # End date
    api_key = 'uaD0spypAvDbh1vlMmrSVfaKAeNUHiNK'

    async with aiohttp.ClientSession() as session:
        data = await fetch_data_in_batches(session, ticker, multiplier, timespan, start_date, end_date, api_key)
        if data is not None:
            print(data)
        else:
            logging.error("No data fetched")


asyncio.run(main())
