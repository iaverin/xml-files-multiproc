
# from ngxmlzip import process
# from ngxmlzip import create
# from ngxmlzip.utils import AllResults
from ngxmlzip import process
from ngxmlzip import create

MAX_OBJECTS_IN_XML = 10
XML_FILES_IN_ZIP = 100
ZIP_FILES = 50
ZIP_DIRECTORY = "zip-files"
CSV_FILE_1 = "csv_file_1.csv"
CSV_FILE_2 = "csv_file_2.csv"


if __name__ == "__main__":
    if not create.create_dir(ZIP_DIRECTORY):
        exit()

    number_of_created_zip_files = 0
    try:
        number_of_created_zip_files = create.create_zip_files(
            MAX_OBJECTS_IN_XML,
            XML_FILES_IN_ZIP,
            ZIP_FILES,
            ZIP_DIRECTORY,
            lambda x: print(f"Created zip file {x}"),
        )
    except OSError as e:
        print(f"Error saving zip file. \n Error: {e}")

    if number_of_created_zip_files != ZIP_FILES:
        print(
            f"Not all zip files was created. Expected {ZIP_FILES}. Created {number_of_created_zip_files}"
        )
        exit()

    print(f"All of {ZIP_FILES} zip files created in {ZIP_DIRECTORY}")

    process_result = process.run_multi_proc(ZIP_DIRECTORY, CSV_FILE_1, CSV_FILE_2)
    print(process_result)
