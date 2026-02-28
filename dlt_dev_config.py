# dlt_dev_config.py
import tomllib  # Python 3.11+, use `tomli` for older versions
from pathlib import Path

def load_dlt_config(path: str = ".dlt/config.toml") -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        return {}
    with cfg_path.open("rb") as f:
        return tomllib.load(f)

def get_pipeline_settings(pipeline_name: str) -> tuple[bool, str]:
    cfg = load_dlt_config()
    pipeline_cfg = cfg.get("pipeline", {}).get(pipeline_name, {})
    # fallback to [env] or [core] if needed
    dev_mode_str = pipeline_cfg.get(
        "dev_mode",
        cfg.get("env", {}).get("dev_mode", "false"),
    )
    dataset_name = pipeline_cfg.get(
        "dataset_name",
        cfg.get("env", {}).get("dataset_name", "bronze"),
    )

    dev_mode = str(dev_mode_str).lower() in ("1", "true", "yes", "on")
    return dev_mode, dataset_name
