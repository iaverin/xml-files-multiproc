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