
from src.status import MainStatus,Main_Status


def main():
    MAIN_STATUS = MainStatus.load_status()
    print(MAIN_STATUS)
    MAIN_STATUS.sync_months()
    if MAIN_STATUS:
        MAIN_STATUS.store_status()


if __name__ == '__main__':
    main()
