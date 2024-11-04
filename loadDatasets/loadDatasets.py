import pandas as pd
import csv
import json
from loadDatasets.dataset import Dataset
from kafka.kafkaSingleton import KafkaProducerSingleton
from confluent_kafka.admin import AdminClient

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
            return

    import pandas as pd
import csv
import json
from loadDatasets.dataset import Dataset
from kafka.kafkaSingleton import KafkaProducerSingleton
from confluent_kafka.admin import AdminClient, NewTopic

JSON_PATH = 'loadDatasets/types.json'

class DatabaseLoader:
    def __init__(self):
        # Get the singleton producer instance
        producer_singleton = KafkaProducerSingleton()
        self.producer = producer_singleton.get_producer()
        self.admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    def delivery_report(self, err, msg):
        """ 
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().     
        """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            return

    def create_topic(self, topic_name):
        """Create a Kafka topic if it does not exist."""
        topic_list = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
        fs = self.admin_client.create_topics(topic_list)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} created")
            except Exception as e: #If the topic already exists
                return
    def load_data(self, dataset_name: str):
        """
        Loads data from a CSV file and sends it to Kafka.
        """
        try:
            with open(JSON_PATH, 'r') as file:
                data = json.load(file)
                file_path = data[dataset_name]["file"]
                topic = data[dataset_name]["topic"]

                # Create the topic if it does not exist
                self.create_topic(topic)
                
                
                # Read CSV with options to handle multiline fields
                df = pd.read_csv(file_path, 
                    escapechar='\\', 
                    skip_blank_lines=True, 
                    engine='python') 
                
                # General data cleaning
                df.dropna(how='all', inplace=True)  # Remove completely empty rows
                df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # Trim whitespace
                df.drop_duplicates(inplace=True)  # Remove duplicate rows
                
                # Replace NaN with 0
                df.fillna(0, inplace=True)
                
                # Instantiate the Dataset class
                dataset = Dataset(name=dataset_name, df=df)
                self.sendToKafka(topic, dataset.get_df())  # send the dataframe to kafka
        except KeyError:
            print(f"Dataset name '{dataset_name}' not found in types.json")
        except Exception as e:
            print(f"An error occurred: {e}")

    def checkIfTopicExists(self, topic):
        """Check if a Kafka topic exists using AdminClient."""
        try:
            # Initialize AdminClient with the necessary broker configuration
            admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
            metadata = admin_client.list_topics(timeout=10)
            return topic+"_topic" in metadata.topics
        except Exception as e:
            print(f"An error occurred while checking if the Kafka topic {topic} exists: {e}")
            return False


    def loadCompanies(self):
        if not self.checkIfTopicExists("companies"):
            self.load_data("companies")

    
    def loadBadges(self):
        if not self.checkIfTopicExists("badges"):
            self.load_data("badges")
    
    def loadFounders(self):
        if not self.checkIfTopicExists("founders"):
            self.load_data("founders")

    def loadIndustries(self):
        if not self.checkIfTopicExists("industries"):
            self.load_data("industries")
    
    def loadPriorCompanies(self):
        if not self.checkIfTopicExists("prior_companies"):
            self.load_data("prior_companies")
    
    def loadRegions(self):
        if not self.checkIfTopicExists("regions"):
            self.load_data("regions")
    
    def loadSchools(self):
        if not self.checkIfTopicExists("schools"):
            self.load_data("schools")
    
    def loadTags(self):
        if not self.checkIfTopicExists("tags"):
            print("yessir")
            self.load_data("tags")
    
    # Kafka Sender
    def sendToKafka(self, topic: str, df: pd.DataFrame):
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            message = json.dumps(row_dict)
            self.producer.produce(topic, value=message, callback=self.delivery_report)
        self.producer.flush()
