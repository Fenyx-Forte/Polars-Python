def read_query(path: str) -> str:
    query = ""
    with open(path, "r") as file:
        query = file.read()

    return query
