"""
Queries for the companies dataset 
"""

from loadDatasets.load_datasets import DatabaseLoader
from kafka import KafkaConsumer
import json
import pandas as pd



def filter_by_industry(industry, topic='companies_topic', bootstrap_servers='localhost:9092'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # List to store filtered data
    filtered_data = []

    # Process messages from Kafka
    for message in consumer:
        company_data = message.value
        if company_data.get("industry") == industry:
            filtered_data.append(company_data)

    # Convert the filtered data to a DataFrame
    filtered_df = pd.DataFrame(filtered_data)
    return filtered_df
