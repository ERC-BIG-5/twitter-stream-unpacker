
from src.status import MainStatus,Main_Status


def main():
    MAIN_STATUS = MainStatus.load_status()
    print(MAIN_STATUS)
    MAIN_STATUS.sync_months()
    MAIN_STATUS.print_database_statuses()
    if MAIN_STATUS:
        MAIN_STATUS.store_status()


if __name__ == '__main__':
    main()
