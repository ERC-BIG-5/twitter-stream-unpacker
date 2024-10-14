import jsonlines

from src.consts import AUTO_RELEVANT_COLLECTION
from src.util import get_post_text

if __name__ == "__main__":
    file = AUTO_RELEVANT_COLLECTION / "2022-02-en.jsonl"
    print(file.absolute().as_posix())
    reader = jsonlines.Reader(file.open())
    c = 0
    for line in reader:
        # data = line[1]
        print(line[2], get_post_text(data))
        # print("---")
        c += 1
    print(c)
