from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENVIRONMENTS_FILE = PROJECT_ROOT / "config" / "environments.yml"
DATASETS_FILE = PROJECT_ROOT / "config" / "datasets.yml"


def load_yaml(file_path: Path) -> dict:
    with file_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_environment(environment: str) -> dict:
    config = load_yaml(ENVIRONMENTS_FILE)

    if environment not in config["environments"]:
        raise ValueError(f"Unknown environment: {environment}")

    return config["environments"][environment]


def get_datasets() -> dict:
    config = load_yaml(DATASETS_FILE)
    return config["datasets"]