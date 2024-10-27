import pandas as pd
import csv
from confluent_kafka import Producer, Consumer
import json
from loadDatasets.dataset import Dataset
from kafka.kafkaSingleton import KafkaProducerSingleton

JSON_PATH = 'loadDatasets/types.json'

class DatabaseLoader:
    def __init__(self):
        # Get the singleton producer instance
        producer_singleton = KafkaProducerSingleton()
        self.producer = producer_singleton.get_producer()

    def delivery_report(self, err, msg):
        """ 
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().     
        """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def load_data(self, dataset_name: str):
        """
        Loads data from a CSV file and sends it to Kafka.
        """
        try:
            with open(JSON_PATH, 'r') as file:
                data = json.load(file)
                file_path = data[dataset_name]["file"]
                topic = data[dataset_name]["topic"]
                
                print(f"Loading data from {file_path} for topic {topic}")
                
                # Automatically detect delimiter
                with open(file_path, 'r') as csvfile:
                    sniffer = csv.Sniffer()
                    sample = csvfile.read(1024)
                    csvfile.seek(0)
                    dialect = sniffer.sniff(sample)
                    delimiter = dialect.delimiter
                
                # Read CSV with options to handle multiline fields
                df = pd.read_csv(file_path, 
                    delimiter=delimiter, 
                    quotechar='"', 
                    escapechar='\\', 
                    skip_blank_lines=True, 
                    engine='python')  # Use Python engine for multiline support
                
                # General data cleaning
                df.dropna(how='all', inplace=True)  # Remove completely empty rows
                df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # Trim whitespace
                df.drop_duplicates(inplace=True)  # Remove duplicate rows
                
                # Replace NaN with 0
                df.fillna(0, inplace=True)
                

                
                # Instantiate the Dataset class
                dataset = Dataset(name=dataset_name, df=df)
                self.send_to_kafka(topic, dataset.get_df())  # send the dataframe to kafka
        except KeyError:
            print(f"Dataset name '{dataset_name}' not found in types.json")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Dataset Loaders
    def load_companies(self):
        self.load_data("companies")
    
    def load_badges(self):
        self.load_data("badges")
    
    def load_founders(self):
        self.load_data("founders")

    def load_industries(self):
        self.load_data("industries")
    
    def load_prior_companies(self):
        self.load_data("prior_companies")
    
    def load_regions(self):
        self.load_data("regions")
    
    def load_schools(self):
        self.load_data("schools")
    
    def load_tags(self):
        self.load_data("tags")
    
    # Kafka Sender
    def send_to_kafka(self, topic: str, df: pd.DataFrame):
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            message = json.dumps(row_dict)
            self.producer.produce(topic, value=message, callback=self.delivery_report)
        self.producer.flush()
