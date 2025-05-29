import argparse
import asyncio
from datetime import datetime
import httpx
import json
from tqdm import tqdm
from typing import Any, Dict

from generate_message import generate_ara_message

ars_url = "https://ars.ci.transltr.io/ars/api"

output = {
    "sequential": {},
    "concurrent": {},
}

MAX_QUERY_TIME = 3600


async def lookup(url: str) -> Dict[str, Any]:
    """Run a single query lookup asynchronously."""
    query = generate_ara_message()
    status = "timeout"
    start_time = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=600) as client:
            response = await client.post(
                f"{url}/submit",
                json=query,
            )
            status = response.status_code
            response.raise_for_status()
            response = response.json()
            parent_pk = response.get("pk", "")
            await asyncio.sleep(10)
            current_time = datetime.now()
            num_results = 0
            while (current_time - start_time).total_seconds() <= MAX_QUERY_TIME:
                res = await client.get(f"{url}/messages/{parent_pk}?trace=y")
                res.raise_for_status()
                response = res.json()
                status = response.get("status")
                if status == "Done" or status == "Error":
                    merged_pk = response.get("merged_version")
                    if merged_pk is None:
                        print(
                            f"Failed to get the ARS merged message from pk: {parent_pk}."
                        )
                        num_results = 0
                    else:
                        # get full merged pk
                        res = await client.get(
                            f"{url}/messages/{merged_pk}"
                        )
                        res.raise_for_status()
                        merged_message = res.json()
                        num_results = len((merged_message.get("fields", {}).get("data", {}).get("message") or {}).get("results") or [])
                    break
                else:
                    current_time = datetime.now()
                    await asyncio.sleep(10)
    except Exception as e:
        num_results = 0

    stop_time = datetime.now()
    result = {
        "status": status,
        "num_results": num_results,
        "response_time": (stop_time - start_time).total_seconds(),
    }
    return result


async def run_sequential(output_filename: str):
    """Send squential queries to kps."""
    if "ars" not in output["sequential"]:
        output["sequential"]["ars"] = {}
    results = []
    start_time = datetime.now()
    for ndx in tqdm(range(0, 15)):
        result = await lookup(ars_url)
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
    output["sequential"]["ars"] = result_stats
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


async def run_concurrent(output_filename: str):
    """Send concurrent async queries to kps."""
    for num in [10, 50, 100, 1000]:
        if "ars" not in output["concurrent"]:
            output["concurrent"]["ars"] = {}
        output["concurrent"]["ars"][num] = {}
        queries = []
        for ndx in range(0, num):
            queries.append(lookup(ars_url))
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
        output["concurrent"]["ars"][num] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)
        # just to give the ARS a little bit to "cool down"
        await asyncio.sleep(300)


async def main():
    parser = argparse.ArgumentParser(description=("Translator Stress Tester"))

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"results/ars_tests_{timestamp}.json"

    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)

    await run_sequential(output_filename)
    await run_concurrent(output_filename)
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


if __name__ == "__main__":
    asyncio.run(main())
