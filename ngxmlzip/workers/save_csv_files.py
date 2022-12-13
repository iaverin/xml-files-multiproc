
import csv
from typing import Any, List
from ..queue_manager import WorkerResult, Worker, ChunkedWorker 

class CSVFile2ChunkedWorker(ChunkedWorker):
    def __init__(self, name: str, csv_file: str, max_chunk_size: int = 0):
        super().__init__(name, max_chunk_size)
        self.csv_file = csv_file

    def worker(self, data: List[Any]) -> WorkerResult:
        records_stored = 0
        with open(self.csv_file, "a", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            for chunk in data:
                for object_name in chunk.object_names:
                    writer.writerow([chunk.id, object_name])
                    records_stored += 1
        return WorkerResult(records_processed=records_stored)


class CSVFile1Worker(Worker):
    def __init__(self, name: str, csv_file: str):
        super().__init__(name)
        self.csv_file = csv_file

    def worker(self, data: Any) -> WorkerResult:
        with open(self.csv_file, "a", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow([data.id, data.level])
            return WorkerResult(records_processed=1)