import ioany

def read_ints(path):
    lines = ioany.read_lines(path)
    yield from (int(_) for _ in lines)

