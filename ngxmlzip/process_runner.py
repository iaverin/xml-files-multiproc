from pprint import pprint
import time
from typing import (
    Any,
    List,
    Generator,
)
import glob
import zipfile
import os
from dataclasses import dataclass
import csv
import multiprocessing

from .data_types import OperationResult, XMLFile
from .queue_manager import (
    QueueWorkersManager,
)
from ngxmlzip.workers.zip_files import put_xml_from_zip_files_in_queue
from ngxmlzip.workers.parse_xml import ParseXMLWorker
from ngxmlzip.workers.save_csv_files import CSVFile1Worker, CSVFile2ChunkedWorker


def create_csv_file_type_1(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "level"])


def create_csv_file_type_2(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "object_name"])


def run_multi_proc(zip_dir, csv_file_1, csv_file_2) -> OperationResult:
    create_csv_file_type_1(csv_file_1)
    create_csv_file_type_2(csv_file_2)

    xml_data_queue = multiprocessing.Manager().Queue()
    data_file_1_queue = multiprocessing.Manager().Queue()
    data_file_2_queue = multiprocessing.Manager().Queue()

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    qm = QueueWorkersManager(multiprocessing.cpu_count())

    parse_xml_worker = ParseXMLWorker(
        name="parse_xml",
        data_file_1_queue=data_file_1_queue,
        data_file_2_queue=data_file_2_queue,
    )

    qm.register_worker(
        xml_data_queue,
        parse_xml_worker,
        on_finish_stop_queues=[
            data_file_1_queue,
            data_file_2_queue,
        ],
        instances=multiprocessing.cpu_count() // 2,
    )

    csv_file_1_worker = CSVFile1Worker(name="csv_file_1", csv_file=csv_file_1)
    qm.register_worker(
        data_file_1_queue,
        csv_file_1_worker,
        instances=1,
    )

    csv_file_2_worker = CSVFile2ChunkedWorker(
        name="csv_file_2", csv_file=csv_file_2, max_chunk_size=1000
    )
    qm.register_worker(data_file_2_queue, csv_file_2_worker, instances=1)

    qm.start_workers(pool)

    try:
        print("Please wait while processing...")

        zip_extraction_results = put_xml_from_zip_files_in_queue(
            zip_dir,
            xml_data_queue,
            # monitoring_queue,
        )

        print(".. zip files extracted")
        print(".. queues are being processed")

        qm.stop_worker_instances_for_queue(xml_data_queue)

        while not qm.workers_finished():
            continue
        results = qm.collect_results()

        pool.close()
        pool.join()

        for worker in (
            parse_xml_worker,
            csv_file_1_worker,
            csv_file_2_worker,
        ):
            print("Workers stats:")
            pprint(qm.worker_results(results, worker))

        return OperationResult(
            total_zip_files=zip_extraction_results.total_zip_files,
            total_xml_files=zip_extraction_results.total_xml_files,
            total_objects=qm.worker_results(
                results, csv_file_2_worker
            ).records_processed,
        )

    except KeyboardInterrupt:
        print("Main Caught KeyboardInterrupt, terminating workers")
        pool.terminate()


if __name__ == "__main__":
    # cProfile.run('run()')
    ZIP_DIRECTORY = "zip-files"
    if os.path.split(os.getcwd())[1].split(os.sep)[-1] == "ngxmlzip":
        zip_dir = f"../{ZIP_DIRECTORY}"
    else:
        zip_dir = f"{ZIP_DIRECTORY}"

    # profiler = cProfile.Profile()
    # profiler.enable()
    t_start = time.time()
    run_multi_proc(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    t_finish = time.time()
    print(f"Time spent {t_finish - t_start}")
    # run(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats("cumtime")
    # stats.print_stats()
