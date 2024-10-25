"""
Analyzing YCombinator companies and what they have in common
"""
import loadDatasets.load_datasets as load_datasets
import queries.companiesQueries as companiesQueries

def main():
    # Initialize the database loader and load datasets
    loader = load_datasets.DatabaseLoader()
    loader.load_companies()

    # Get companies by industry using the correct function
    companies_by_industry = companiesQueries.filter_by_industry(loader.companies.get_df(), "Health")
    print(companies_by_industry)

if __name__ == "__main__":
    main()
