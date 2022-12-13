import glob
import multiprocessing
from typing import Generator, List
import zipfile

from ..data_types import OperationResult, XMLFile



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