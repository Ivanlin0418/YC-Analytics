from loadDatasets.dataset import Dataset
import pandas as pd

class DatabaseLoader:
    def __init__(self):
        self.companies = None
        self.badges = None
        self.founders = None

    def load_companies(self):
        df = pd.read_csv("datasets/companies.csv")
        self.companies = Dataset(name="companies", df=df)

    def load_badges(self):
        df = pd.read_csv("datasets/badges.csv")
        self.badges = Dataset(name="badges", df=df)

    def load_founders(self):
        df = pd.read_csv("datasets/founders.csv")
        self.founders = Dataset(name="founders", df=df)

    def load_industries(self):
        df = pd.read_csv("datasets/industries.csv")
        self.industries = Dataset(name="industries", df=df)
    
    def load_prior_companies(self):
        df = pd.read_csv("datasets/prior_companies.csv")
        self.prior_companies = Dataset(name="prior_companies", df=df)

    def load_regions(self):
        df = pd.read_csv("datasets/regions.csv")
        self.regions = Dataset(name="regions", df=df)
    
    def load_schools(self):
        df = pd.read_csv("datasets/schools.csv")
        self.schools = Dataset(name="schools", df=df)

    def load_tags(self):
        df = pd.read_csv("datasets/tags.csv")
        self.tags = Dataset(name="tags", df=df)


