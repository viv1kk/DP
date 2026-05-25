from exceptions.RecoverableError import RecoverableError

class BasePipeline:
    def run(self):
        data = self.extract()

        data = self.transform(data)

        data = self.validate(data)

        self.load(data)


    def extract(self):
        raise NotImplementedError

    def transform(self, df):
        return df

    def validate(self, df):
        return df

    def load(self, df):
        raise NotImplementedError