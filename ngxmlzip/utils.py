from dataclasses import dataclass
from typing import List


@dataclass
class XMLFile:
    zip_file: str
    xml_file: str
    xml_data: str
@dataclass
class ParsedXMLData:
    id: str
    level: str
    object_names: List[str]


@dataclass
class DataCSVFile1:
    id: str
    level: str


@dataclass
class DataCSVFile2:
    id: str
    object_names: List[str]

@dataclass
class OperationResult:
    total_zip_files: int = 0
    total_xml_files: int = 0
    total_objects: int = 0


class TextColors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_results(
    total_zip_files: int,
    xml_files_proceeded,
    csv_file_1,
    csv_file_1_records,
    csv_file_2,
    csv_file_2_records,
):
    print(f"=======================================================================")
    print(f"Total zip_file {total_zip_files}")
    print(f"XML files processed {xml_files_proceeded}")
    print(f"Records in {csv_file_1} stored {csv_file_1_records}")
    print(f"Records in {csv_file_2} stored {csv_file_2_records}")
