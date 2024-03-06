from patex.nodes.node import PythonNode


class CombineDicts(PythonNode):
    def __init__(self):
        pass

    def apply(self, *args) -> dict:
        d = {}
        for arg in args:
            d.update(arg)
        return d
