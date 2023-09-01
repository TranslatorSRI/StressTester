import argparse
import asyncio
from datetime import datetime
import httpx
import json
from typing import Any, Dict, List

from generate_message import generate_message

with open("curie_list.json", "r") as f:
    all_curies = json.load(f)

with open("kps.json", "r") as f:
    kps = json.load(f)

output = {
    "sequential": {},
    "concurrent": {},
}


def single_lookup(url: str, curies: List[str], kp_overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Run a single query lookup synchronously."""
    query = generate_message(curies, kp_overrides)
    status = "timeout"
    try:
        with httpx.Client(timeout=600) as client:
            start_time = datetime.now()
            response = client.post(
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
        "num_curies": len(curies),
        "curies": curies,
    }
    return result


async def single_async_lookup(url: str, curies: List[str], kp_overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Run a single query lookup asynchronously."""
    query = generate_message(curies, kp_overrides)
    status = "timeout"
    try:
        async with httpx.AsyncClient(timeout=600) as client:
            start_time = datetime.now()
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
        "num_curies": len(curies),
        "curies": curies,
    }
    return result


def run_sequential(num_curies: int, output_filename: str):
    """Send squential queries to kps."""
    for kp, kp_overrides in kps.items():
        if kp not in output["sequential"]:
            output["sequential"][kp] = {}
        output["sequential"][kp][num_curies] = {}
        results = []
        start_time = datetime.now()
        for ndx in range(0, len(all_curies), num_curies):
            curies = all_curies[ndx : min(ndx + num_curies, len(all_curies))]
            result = single_lookup(kp, curies, kp_overrides)
            results.append(result)
        # await asyncio.gather(*map(single_lookup, queries), return_exceptions=True)
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
        output["sequential"][kp][num_curies] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


async def run_concurrent(num_curies: int, output_filename: str):
    """Send concurrent async queries to kps."""
    for kp, kp_overrides in kps.items():
        if kp not in output["concurrent"]:
            output["concurrent"][kp] = {}
        output["concurrent"][kp][num_curies] = {}
        query_curies = []
        for ndx in range(0, len(all_curies), num_curies):
            query_curies.append(all_curies[ndx : min(ndx + num_curies, len(all_curies))])
        lookups = [single_async_lookup(kp, curies, kp_overrides) for curies in query_curies]
        start_time = datetime.now()
        results = await asyncio.gather(*lookups, return_exceptions=True)
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
        output["concurrent"][kp][num_curies] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


def main():
    parser = argparse.ArgumentParser(description=("Translator KP Stress Tester"))
    parser.add_argument("--batch_sizes", type=str, help="Comma separated curie batch sizes", default="1,10,100,1000")
    args = parser.parse_args()
    batch_sizes = [int(batch_size) for batch_size in args.batch_sizes.split(",")]

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"kp_tests_{timestamp}.json"

    for num_curies in batch_sizes:
        run_sequential(num_curies, output_filename)
        asyncio.run(run_concurrent(num_curies, output_filename))
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


if __name__ == "__main__":
    main()
