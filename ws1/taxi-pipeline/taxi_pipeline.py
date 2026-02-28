import dlt
import requests
from typing import Iterator, List, Dict, Any

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


def _get_page(page: int) -> List[Dict[str, Any]]:
    response = requests.get(BASE_URL, params={"page": page}, timeout=30)
    response.raise_for_status()
    return response.json()


@dlt.resource(name="taxi_data", write_disposition="replace")
def nyc_taxi_trips() -> Iterator[List[Dict[str, Any]]]:
    page = 1
    while True:
        data = _get_page(page)
        if not data:
            break
        yield data
        page += 1


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )
    load_info = pipeline.run(nyc_taxi_trips())
    print(load_info)


if __name__ == "__main__":
    run()
