"""
Analyzing YCombinator companies and what they have in common
"""
from loadDatasets.loadDatasets import DatabaseLoader
from dataAnalytics import DataAnalytics


def main():
    # Initialize the database loader
    loader = DatabaseLoader()
    loader.load_companies() 
    # loader.load_industries()
    loader.load_founders()
    # loader.load_prior_companies()
    # loader.load_regions()
    # loader.load_schools()
    # loader.load_tags()

    dataAnalytics = DataAnalytics()
    # print (dataAnalytics.getMostSuccessfulCompaniesFromKafka())

    # print (dataAnalytics.getMostSuccessfulFoundersFromKafka())
    # print (dataAnalytics.getMostSuccessfulCompaniesFromKafka())
    print (dataAnalytics.getMostSuccessFoundersAndTheirCompaniesFromKafka())
    # print (dataAnalytics.getMostSuccessfulCompaniesWithFoundersFromKafka())


if __name__ == "__main__":
    main()

