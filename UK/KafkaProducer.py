import asyncio
import aiohttp
import json
from kafka import KafkaProducer
from urllib.parse import urlencode
from aiohttp import ClientResponseError
from time import time
import os
import pandas as pd 

BASE_API_URL = "https://data.police.uk/api"
CRIME_ENDPOINT = "/crimes-street/all-crime"
BOOTSTRAP_SERVERS = "localhost:9092"
MONTHS = [
    "2023-03", "2023-04", "2023-05", "2023-06",
    "2023-07", "2023-08", "2024-09", "2024-10",
    "2024-11", "2024-12", "2025-01", "2025-02",
]
MAX_CONCURRENT_REQUESTS = 10  
MAX_CONCURRENT_FORCES = 6  
MAX_RETRIES = 5  
INITIAL_RETRY_DELAY = 0.5  
RATE_LIMIT_PER_SECOND = 15  
DELAY_PER_REQUEST = 0.09  
NEIGHBOURHOODS_CACHE_FILE = "neighbourhoods_cache.json"


CSV_FILE_PATH = "D:/Code/Python/CrimeDataAnalysis/Crime_Data_from_2020_to_Present.csv"
CSV_TOPIC = "crime_csv_topic"

async def fetch_forces(session):
    try:
        async with session.get(f"{BASE_API_URL}/forces") as response:
            response.raise_for_status()
            forces = [force["id"] for force in await response.json()]
            return forces
    except Exception as e:
        print(f"Failed to fetch forces: {e}")
        return []

async def fetch_neighbourhoods(session, force, retries=0):
    try:
        async with session.get(f"{BASE_API_URL}/{force}/neighbourhoods") as response:
            if response.status == 429:
                if retries < MAX_RETRIES:
                    delay = INITIAL_RETRY_DELAY * (2 ** retries)
                    print(f"429 Too Many Requests for neighbourhoods {force}. Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    return await fetch_neighbourhoods(session, force, retries + 1)
                else:
                    print(f"Max retries reached for neighbourhoods {force}. Skipping.")
                    return []
            response.raise_for_status()
            neighbourhoods = [neighbourhood["id"] for neighbourhood in await response.json()]
            return neighbourhoods
    except ClientResponseError as e:
        if e.status == 429:
            if retries < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** retries)
                print(f"429 Too Many Requests for neighbourhoods {force}. Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                return await fetch_neighbourhoods(session, force, retries + 1)
            else:
                print(f"Max retries reached for neighbourhoods {force}. Skipping.")
                return []
        print(f"Error fetching neighbourhoods for {force}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching neighbourhoods for {force}: {e}")
        return []

def load_neighbourhoods_cache():
    if os.path.exists(NEIGHBOURHOODS_CACHE_FILE):
        with open(NEIGHBOURHOODS_CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_neighbourhoods_cache(cache):
    with open(NEIGHBOURHOODS_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

async def get_neighbourhoods(session, force, cache):
    if force in cache:
        print(f"Using cached neighbourhoods for {force}")
        return cache[force]
    neighbourhoods = await fetch_neighbourhoods(session, force)
    if neighbourhoods:
        cache[force] = neighbourhoods
        save_neighbourhoods_cache(cache)
    return neighbourhoods

async def fetch_and_process_boundary(session, force, neighbourhood, retries=0):
    try:
        processed_neighbourhood = neighbourhood.replace(" ", "%20")
        async with session.get(f"{BASE_API_URL}/{force}/{processed_neighbourhood}/boundary") as response:
            if response.status == 429:
                if retries < MAX_RETRIES:
                    delay = INITIAL_RETRY_DELAY * (2 ** retries)
                    print(f"429 Too Many Requests for {force}/{neighbourhood}. Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    return await fetch_and_process_boundary(session, force, neighbourhood, retries + 1)
                else:
                    print(f"Max retries reached for {force}/{neighbourhood}. Skipping.")
                    return None
            if response.status != 200:
                print(f"Failed to fetch boundary for {force}/{neighbourhood}: {response.status}")
                return None
            boundary_array = await response.json()
            lat_lng_points = [f"{point['latitude']},{point['longitude']}" for point in boundary_array]
            if lat_lng_points:
                lat_lng_points.append(lat_lng_points[0])
            return ":".join(lat_lng_points)
    except ClientResponseError as e:
        if e.status == 429:
            if retries < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** retries)
                print(f"429 Too Many Requests for {force}/{neighbourhood}. Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                return await fetch_and_process_boundary(session, force, neighbourhood, retries + 1)
            else:
                print(f"Max retries reached for {force}/{neighbourhood}. Skipping.")
                return None
        print(f"Error fetching boundary for {force}/{neighbourhood}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching boundary for {force}/{neighbourhood}: {e}")
        return None

async def fetch_and_produce_crimes(session, producer, poly, force, neighbourhood, date, retries=0):
    try:
        form_data = {"poly": poly, "date": date}
        async with session.post(
            f"{BASE_API_URL}{CRIME_ENDPOINT}",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=urlencode(form_data)
        ) as response:
            if response.status == 429:
                if retries < MAX_RETRIES:
                    delay = INITIAL_RETRY_DELAY * (2 ** retries)
                    print(f"429 Too Many Requests for crimes {force}/{neighbourhood} ({date}). Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    return await fetch_and_produce_crimes(session, producer, poly, force, neighbourhood, date, retries + 1)
                else:
                    print(f"Max retries reached for crimes {force}/{neighbourhood} ({date}). Skipping.")
                    return
            if response.status == 503:
                print(f"Too many crimes (>10,000) for poly: {poly[:50]}... ({date})")
                return
            if response.status != 200:
                print(f"Crime request failed for {force}/{neighbourhood} ({date}): {response.status}")
                return
            crime_array = await response.json()
            for crime in crime_array:
                crime_json = json.dumps(crime)
                producer.send(date, value=crime_json.encode("utf-8"))
            producer.flush()
    except ClientResponseError as e:
        if e.status == 429:
            if retries < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** retries)
                print(f"429 Too Many Requests for crimes {force}/{neighbourhood} ({date}). Retrying after {delay:.2f}s (attempt {retries + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                return await fetch_and_produce_crimes(session, producer, poly, force, neighbourhood, date, retries + 1)
            else:
                print(f"Max retries reached for crimes {force}/{neighbourhood} ({date}). Skipping.")
                return
        print(f"Error fetching crimes for {force}/{neighbourhood} ({date}): {e}")
    except Exception as e:
        print(f"Unexpected error fetching crimes for {force}/{neighbourhood} ({date}): {e}")

async def process_neighbourhood(session, producer, force, neighbourhood, force_index, neighbourhood_index, date, semaphore):
    async with semaphore:
        poly = await fetch_and_process_boundary(session, force, neighbourhood)
        if poly:
            print(f"Sending data of [{force_index}] {force} [{neighbourhood_index + 1}] {neighbourhood} for {date}")
            await fetch_and_produce_crimes(session, producer, poly, force, neighbourhood, date)
        await asyncio.sleep(DELAY_PER_REQUEST)

async def process_force(session, producer, force, force_index, date, semaphore, cache):
    start_time = time()
    neighbourhoods = await get_neighbourhoods(session, force, cache)
    print(f"Processing {force} with {len(neighbourhoods)} neighbourhoods for {date}")
    
    tasks = [
        process_neighbourhood(session, producer, force, neighbourhood, force_index, i, date, semaphore)
        for i, neighbourhood in enumerate(neighbourhoods)
    ]
    
    await asyncio.gather(*tasks)
    
    elapsed_time = time() - start_time
    print(f"Processed {force} in {elapsed_time:.2f}s for {date}")

async def process_month(session, producer, date, semaphore, cache):
    start_month_time = time()
    print(f"\nStarting data collection for {date}")
    
    forces = await fetch_forces(session)
    print(f"Found forces: {len(forces)} for {date}")

    for i in range(0, len(forces), MAX_CONCURRENT_FORCES):
        batch_forces = forces[i:i + MAX_CONCURRENT_FORCES]
        tasks = [
            process_force(session, producer, force, i + 1, date, semaphore, cache)
            for i, force in enumerate(batch_forces, start=i)
        ]
        await asyncio.gather(*tasks)
    
    month_time = time() - start_month_time
    print(f"Completed {date} in {month_time:.2f} seconds ({month_time / 60:.2f} minutes)")
    
    
def push_csv_to_kafka(producer):
    print(f"Pushing CSV data to Kafka topic {CSV_TOPIC}...")
    df = pd.read_csv(CSV_FILE_PATH)
    for _, row in df.iterrows():
        row_json = row.to_json()
        producer.send(CSV_TOPIC, value=row_json.encode("utf-8"))
    producer.flush()
    print(f"Finished pushing CSV data to Kafka topic {CSV_TOPIC}.")

async def main():
    start_total_time = time()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k if isinstance(k, bytes) else k.encode("utf-8") if k else None,
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8") if v else None,
        batch_size=16384,
        linger_ms=10
    )
    
    push_csv_to_kafka(producer=producer)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    cache = load_neighbourhoods_cache()

    async with aiohttp.ClientSession() as session:
        for date in MONTHS:
            await process_month(session, producer, date, semaphore, cache)

    producer.close()

    total_time = time() - start_total_time
    print(f"\nTotal execution time for all months: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)")

if __name__ == "__main__":
    asyncio.run(main())