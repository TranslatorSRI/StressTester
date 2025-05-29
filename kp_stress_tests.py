import argparse
import asyncio
import copy
from datetime import datetime
import httpx
import json
from tqdm import tqdm
from typing import Any, Dict, List

with open("kp_queries.json", "r") as f:
    kps = json.load(f)

output = {
    "sequential": {},
    "concurrent": {},
}


def generate_message(query, num_curies):
    nodes = query["message"]["query_graph"]["nodes"]
    for node in nodes.values():
        if "ids" in node:
            node["ids"] = node["ids"][:num_curies]
    query["bypass_cache"] = True
    return query


async def lookup(kp: dict, num_curies: int) -> Dict[str, Any]:
    """Run a single query lookup asynchronously."""
    status = "timeout"
    start_time = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=600) as client:
            response = await client.post(
                kp["url"],
                json=generate_message(copy.deepcopy(kp["query"]), num_curies),
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


async def run_sequential(infores: str, kp: dict, output_filename: str):
    """Send squential queries to kps."""
    # for num in [(1, 1000)]:
    for num in [(900, 1), (180, 10), (90, 100), (30, 1000)]:
        print(f"Sending {num} sequential requests to {infores}")
        if infores not in output["sequential"]:
            output["sequential"][infores] = {}
        output["sequential"][infores][f"{num[0]}_{num[1]}"] = {}
        results = []
        start_time = datetime.now()
        for ndx in range(0, num[0]):
            result = await lookup(kp, num[1])
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
        output["sequential"][infores][f"{num[0]}_{num[1]}"] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


async def run_concurrent(infores: str, kp: dict, output_filename: str):
    """Send concurrent async queries to kps."""
    for num in [(10, 1), (100, 1), (1000, 1), (10, 10), (100, 10), (1000, 10), (10, 1000), (100, 1000), (1000, 1000)]:
        print(f"Sending {num[0]} concurrent requests to {infores}")
        if infores not in output["concurrent"]:
            output["concurrent"][infores] = {}
        output["concurrent"][infores][f"{num[0]}_{num[1]}"] = {}
        lookups = [lookup(kp, num[1]) for _ in range(0, num[0])]
        start_time = datetime.now()
        results = await asyncio.gather(*lookups)
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
        output["concurrent"][infores][f"{num[0]}_{num[1]}"] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


async def run_tests(infores: str, kp: dict, output_filename: str):
    await run_sequential(infores, kp, output_filename)
    # let the KPs cool off a little
    await asyncio.sleep(60)
    await run_concurrent(infores, kp, output_filename)


async def main():
    parser = argparse.ArgumentParser(description=("Translator Stress Tester"))

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"results/stress_tests_{timestamp}.json"

    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)

    tests = [run_tests(infores, kp, output_filename) for infores, kp in kps.items()]
    await asyncio.gather(*tests)

        
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


if __name__ == "__main__":
    asyncio.run(main())
