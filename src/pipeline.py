from src.ingestion.fetch_arrivals import fetch_arrivals
from src.transform.parse_arrivals import parse_all_arrivals
from src.storage.load_arrivals import load_arrivals_data
from src.utils.archive import archive_files

def main():
    fetch_arrivals()

    df, processed_files = parse_all_arrivals("data/raw/arrivals")

    load_arrivals_data(df)

    archive_files(processed_files)


if __name__ == "__main__":
    main()