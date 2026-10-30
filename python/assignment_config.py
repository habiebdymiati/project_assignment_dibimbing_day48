import logging

# Konfigurasi Logging disesuaikan dengan format screenshot: YYYY-MM-DD HH:MM:SS
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger_prod = logging.getLogger('producer')
logger_cons = logging.getLogger('consumer')

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'events_topic'
CONSUMER_GROUP_ID = 'events_group'
PRODUCER_INTERVAL_SECONDS = 5
USERS_LIST = ['user_1', 'user_2', 'user_3']
DATA_SINK_FILE = './output/output.csv'
