import pandas as pd


class Source:
    def __init__(self, dataset):
        self.dataset = dataset
        self.dataraw = None
        self.__load()

    def __load(self):
        self.dataraw = pd.read_csv(self.dataset,
                                   encoding='latin-1', sep=',')


class DataWrangling(Source):
    def __init__(self, dataset):
        super().__init__(dataset)
        self.process()

    def process(self):
        data = self.dataraw
        data['newspaper_uid'] = 'eluniversal'
        print(data)
