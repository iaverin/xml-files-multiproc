import csv
import os


def create_dir(dir) -> bool:
    try:
        os.mkdir(dir)
        return True
    except FileExistsError:
        return True
    except OSError as e:
        print(f"Could not create directory {dir}. \n Error: {e}")
        return False


def create_csv_file_type_1(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "level"])


def create_csv_file_type_2(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "object_name"])
