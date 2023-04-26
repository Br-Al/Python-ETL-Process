class TransformationStrategy:
    def transform(self, data, transformer):
        pass


class SimpleStrategy(TransformationStrategy):
    def transform(self, data, transformer):
        return transformer.transform(data)


class ComplexStrategy(TransformationStrategy):
    def transform(self, data, transformer):
        return transformer.transform(data)
