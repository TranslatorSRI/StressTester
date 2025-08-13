import argparse
import asyncio
from datetime import datetime
import httpx
import json
from tqdm import tqdm
from typing import Any, Dict, List

from generate_message import generate_kp_message

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
    query = generate_kp_message(curies, kp_overrides)
    status = "timeout"
    start_time = datetime.now()
    try:
        with httpx.Client(timeout=600) as client:
            response = client.post(
                url,
                json=query,
            )
            status = response.status_code
            response.raise_for_status()
            response = response.json()
            if (
                "message" in response and
                "errors" in response["message"]
            ):
                with open("kp_response.json", "w") as f:
                    json.dump(response, f, indent=2)
                exit()
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
    query = generate_kp_message(curies, kp_overrides)
    status = "timeout"
    start_time = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=600) as client:
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
    for infores, kp in kps.items():
        print(f"Sending {len(all_curies) / num_curies} sequential requests to {infores}")
        if infores not in output["sequential"]:
            output["sequential"][infores] = {}
        output["sequential"][infores][num_curies] = {}
        results = []
        start_time = datetime.now()
        for ndx in tqdm(range(0, len(all_curies), num_curies)):
            curies = all_curies[ndx : min(ndx + num_curies, len(all_curies))]
            result = single_lookup(kp["url"], curies, kp)
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
        output["sequential"][infores][num_curies] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


async def run_concurrent(num_curies: int, output_filename: str):
    """Send concurrent async queries to kps."""
    for infores, kp in kps.items():
        print(f"Sending {len(all_curies) / num_curies} concurrent requests to {infores}")
        if infores not in output["concurrent"]:
            output["concurrent"][infores] = {}
        output["concurrent"][infores][num_curies] = {}
        query_curies = []
        for ndx in range(0, len(all_curies), num_curies):
            query_curies.append(all_curies[ndx : min(ndx + num_curies, len(all_curies))])
        lookups = [single_async_lookup(kp["url"], curies, kp) for curies in query_curies]
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
        output["concurrent"][infores][num_curies] = result_stats
        with open(output_filename, "w") as f:
            json.dump(output, f, indent = 2)


async def main():
    parser = argparse.ArgumentParser(description=("Translator Stress Tester"))
    parser.add_argument("--batch_sizes", type=str, help="Comma separated curie batch sizes", default="1,10,100,1000")
    args = parser.parse_args()
    batch_sizes = [int(batch_size) for batch_size in args.batch_sizes.split(",")]

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"results/kp_tests_{timestamp}.json"

    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)

    for num_curies in batch_sizes:
        run_sequential(num_curies, output_filename)
        await run_concurrent(num_curies, output_filename)
    with open(output_filename, "w") as f:
        json.dump(output, f, indent = 2)


if __name__ == "__main__":
    asyncio.run(main())
