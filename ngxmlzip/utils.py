from dataclasses import dataclass


@dataclass
class OperationResult:
    total_zip_files: int = 0
    total_xml_files: int = 0
    total_objects: int = 0


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
