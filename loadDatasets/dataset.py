class Dataset:
    def __init__(self, name, df):
        self.name = name
        self.df = df

    def get_df(self):
        return self.df
    
    def get_name(self):
        return self.name