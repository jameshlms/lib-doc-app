import json
from pathlib import Path


def get_params(package_name: str) -> dict:
    params_path = Path(__file__).parent.parent / "pipeline-params" / "params.json"
    with params_path.open("r", encoding="utf-8") as f:
        all_params = json.load(f)
    if package_name not in all_params:
        raise KeyError(f"Parameters for package '{package_name}' not found.")
    return all_params[package_name]
