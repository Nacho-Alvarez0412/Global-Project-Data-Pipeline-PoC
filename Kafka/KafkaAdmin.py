from confluent_kafka.admin import (AdminClient, NewTopic, ConfigResource)
from config import config

# Kafka Functions

# return True if topic exists and False if not
def topic_exists(admin, topic):
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False

# create new topic and return results dictionary
def create_topic(admin, topic):
    new_topic = NewTopic(topic, num_partitions=1) 
    result_dict = admin.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

# deletes a topic and return results dictionary            
def delete_topics(admin, topics):
    result_dict = admin.delete_topics(topics, operation_timeout=30)
    # Wait for operation to finish.
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))



if __name__ == '__main__':
    # Create Admin client
    admin = AdminClient(config)
    topics = ['Raw_Transactions','Classified_Transactions']
    # Create topic if it doesn't exist
    #delete_topics(admin,topics)
    for topic in topics:
        if not topic_exists(admin, topic):
            create_topic(admin, topic)
    # Print Existing Topics
    print(admin.list_topics().topics.values())
