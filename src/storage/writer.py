import csv
from pathlib import Path
from typing import Dict, Iterable, List
from .schema import RAW_COLUMNS

class CSVDatasetWriter:
    def __init__(self, csv_path: Path, columns: List[str] = RAW_COLUMNS):
        self.csv_path = Path(csv_path)
        self.columns = columns
        self._ensure_header()

    def _ensure_header(self):
        if not self.csv_path.exists():
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=self.columns)
                w.writeheader()

    def append_rows(self, rows: Iterable[Dict]):
        with open(self.csv_path, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.columns)
            for row in rows:
                filtered = {k: row.get(k, "") for k in self.columns}
                w.writerow(filtered)
