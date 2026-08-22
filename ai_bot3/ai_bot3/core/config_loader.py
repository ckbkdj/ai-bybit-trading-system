from pathlib import Path
import yaml

def load_config(path="config.yml") -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"配置文件缺失: {p.resolve()}")
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f)
