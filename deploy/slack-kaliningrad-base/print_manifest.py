"""Slack CLI get-manifest hook; emits the checked-in static manifest."""
from pathlib import Path

print(Path(__file__).with_name("manifest.json").read_text())
