"""
Analyzing YCombinator companies and what they have in common
"""
from loadDatasets.loadDatasets import DatabaseLoader
from dataAnalytics import DataAnalytics


def main():
    # Initialize the database loader
    loader = DatabaseLoader()
    loader.loadCompanies() 
    loader.loadIndustries()
    loader.loadFounders()
    loader.loadPriorCompanies()
    loader.loadRegions()
    loader.loadSchools()
    loader.loadTags()

    dataAnalytics = DataAnalytics()
    # print (dataAnalytics.getMostSuccessfulCompaniesFromKafka())

    # print (dataAnalytics.getMostSuccessfulFoundersFromKafka())
    # print (dataAnalytics.getKafkaTopicList())
    # print (dataAnalytics.getMostSuccessfulCompaniesFromKafka())
    print (dataAnalytics.getMostSuccessFoundersAndTheirCompaniesFromKafka())


if __name__ == "__main__":
    main()

