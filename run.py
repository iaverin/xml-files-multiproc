from pprint import pprint
from ngxmlzip import process_runner
from ngxmlzip import create_runner
from ngxmlzip.data_types import OperationResult
from ngxmlzip.utils import TextColors

MAX_OBJECTS_IN_XML = 10
XML_FILES_IN_ZIP = 100
ZIP_FILES = 50
ZIP_DIRECTORY = "zip-files"
CSV_FILE_1 = "csv_file_1.csv"
CSV_FILE_2 = "csv_file_2.csv"


if __name__ == "__main__":
    if not create_runner.create_dir(ZIP_DIRECTORY):
        print(f"{TextColors.FAIL}directory {ZIP_DIRECTORY} could not be created!{TextColors.ENDC}")
        exit()

    create_result: OperationResult = None
    try:
        create_result = create_runner.create_zip_files(
            MAX_OBJECTS_IN_XML,
            XML_FILES_IN_ZIP,
            ZIP_FILES,
            ZIP_DIRECTORY,
            lambda x: print(f"Created zip file {x}"),
        )
    except OSError as e:
        print(f"Error saving zip file. \n Error: {e}")
        exit()
        # clean-up code

    if create_result.total_zip_files != ZIP_FILES:
        print(
            f"Not all zip files were created. Expected {ZIP_FILES}. Created {create_result.total_zip_files}"
        )
        exit()

    print(f"All of {ZIP_FILES} zip files created in {ZIP_DIRECTORY}")
    print("================ Create results  ========================")
    pprint(create_result)
    print("================ Process result =========================")
    process_result = process_runner.run_multi_proc(ZIP_DIRECTORY, CSV_FILE_1, CSV_FILE_2)
    pprint(process_result)

    if create_result == process_result:
        print(f"{TextColors.OKGREEN}All Ok{TextColors.ENDC}")
    else:
        print(f"{TextColors.FAIL}Error!{TextColors.ENDC}")
        for k,v in process_result.__dict__.items():
            if create_result.__dict__[k] != v:
                print(f"created {k}: {create_result.__dict__[k]}  processed: {v}")

