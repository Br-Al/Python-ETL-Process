from utils.factories import LoaderFactory, ExtractorFactory
from models.transformers import Transformer
from data.extractors import Extractor
from repositories.loaders import Loader


class Etl:
    def __init__(self, extractor: Extractor, transformer:Transformer, loader: Loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        data = self.extractor.extract_data()
        if self.transformer:
            data = self.transformer.transform(data)
        if not data.empty:
            self.loader.load(data)

