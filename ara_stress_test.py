import argparse
import asyncio
from datetime import datetime
import httpx
import json
from tqdm import tqdm
from typing import Any, Dict

from generate_message import generate_ara_message

with open("aras.json", "r") as f:
    aras = json.load(f)

output = {
    "sequential": {},
    "concurrent": {},
}


async def lookup(url: str) -> Dict[str, Any]:
    """Run a single query lookup asynchronously."""
    query = generate_ara_message()
    status = "timeout"
    start_time = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=3600) as client:
            response = await client.post(
                url,
                json=query,
            )
            status = response.status_code
            response.raise_for_status()
            response = response.json()
            num_results = len((response.get("message") or {}).get("results") or [])
    except Exception as e:
        num_results = 0

    stop_time = datetime.now()
    result = {
        "status": status,
        "num_results": num_results,
        "response_time": (stop_time - start_time).total_seconds(),
    }
    return result


async def run_sequential(infores: str, ara: dict, output_filename: str):
    """Send squential queries to kps."""
    if infores not in output["sequential"]:
        output["sequential"][infores] = {}
    results = []
    start_time = datetime.now()
    for ndx in tqdm(range(0, 15)):
        result = await lookup(ara["url"])
        results.append(result)
    end_time = datetime.now()
    result_stats = {
        "total_time": (end_time - start_time).total_seconds(),
        "total_results": 0,
        "statuses": {},
        "results": results,
    }
    for result in results:
        result_stats["total_results"] += result["num_results"]
        if result["status"] not in result_stats["statuses"]:
            result_stats["statuses"][result["status"]] = 1
        else:
            result_stats["statuses"][result["status"]] += 1
    output["sequential"][infores] = result_stats
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


async def run_concurrent(infores: str, ara: dict, output_filename: str):
    """Send concurrent async queries to kps."""
    for num in [10, 50, 100, 1000]:
        if infores not in output["concurrent"]:
            output["concurrent"][infores] = {}
        output["concurrent"][infores][num] = {}
        queries = []
        for ndx in range(0, num):
            queries.append(lookup(ara["url"]))
        start_time = datetime.now()
        results = await asyncio.gather(*queries, return_exceptions=True)
        end_time = datetime.now()
        result_stats = {
            "total_time": (end_time - start_time).total_seconds(),
            "total_results": 0,
            "statuses": {},
            "results": results,
        }
        for result in results:
            result_stats["total_results"] += result["num_results"]
            if result["status"] not in result_stats["statuses"]:
                result_stats["statuses"][result["status"]] = 1
            else:
                result_stats["statuses"][result["status"]] += 1
        output["concurrent"][infores][num] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)
        # just to give the ARA a little bit to "cool down"
        await asyncio.sleep(30)


async def main():
    parser = argparse.ArgumentParser(description=("Translator Stress Tester"))

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"results/ara_tests_{timestamp}.json"

    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)

    for infores, ara in aras.items():
        # run aras one at a time so KPs don't get completely bashed. That's for the ARS tests.
        print(f"Running sequential tests against {infores}")
        await run_sequential(infores, ara, output_filename)
        print(f"Running concurrent tests against {infores}")
        await run_concurrent(infores, ara, output_filename)
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


if __name__ == "__main__":
    asyncio.run(main())
