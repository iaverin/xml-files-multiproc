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

from .utils import OperationResult, XMLFile
from .queue_manager import (
    QueueWorkersManager,
)
from ngxmlzip.workers.parse_xml import ParseXMLWorker
from ngxmlzip.workers.save_csv_files import CSVFile1Worker, CSVFile2ChunkedWorker


def get_zip_files(directory: str) -> List[str]:
    return glob.glob(directory)


def get_xml_files(zip_file) -> Generator[str, None, None]:
    with zipfile.ZipFile(zip_file, mode="r") as zip:
        files = zip.namelist()
        for f in files:
            yield f


def xml_from_zip(zipfile: zipfile.ZipFile, xml_file_name: str) -> str:
    """
    returns xml file contents from zip file as string
    throws FileNotFoundError if file not found inside zip
    """
    try:
        with zipfile.open(xml_file_name, "r") as xml_file:
            xml_data = xml_file.read().decode("utf-8")
            return xml_data
    except KeyError:
        raise FileNotFoundError(f"File not found inside zip: {xml_file_name}")


def create_csv_file_type_1(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "level"])


def create_csv_file_type_2(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "object_name"])


def put_xml_from_zip_files_in_queue(
    zip_dir: str,
    xml_data_queue: multiprocessing.Queue,
    # monitoring_queue: multiprocessing.Queue,
) -> OperationResult:
    total_zip_files = 0
    total_xml_files = 0

    zip_files = get_zip_files(f"{zip_dir}/*.zip")
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, mode="r") as zip:
            # monitoring_queue.put("zip_file_extracted")
            xml_files = get_xml_files(zip_file)
            total_zip_files += 1
            for xml_file in xml_files:
                # print(f"Zip file {zip_file}. Extracted XML file {xml_file}")
                xml_data = xml_from_zip(zip, xml_file)
                xml_data_queue.put(
                    XMLFile(zip_file=zip_file, xml_file=xml_file, xml_data=xml_data)
                )
                total_xml_files += 1
            #  monitoring_queue.put("xml_file_added")
    return OperationResult(total_zip_files, total_xml_files)


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
