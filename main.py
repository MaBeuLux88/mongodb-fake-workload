from datetime import datetime, timedelta
from time import time, ctime
from random import randint, random
from faker import Faker
from pymongo import ASCENDING
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# CONSTS
NB_USERS = 1000
RESET_DB_EVERY_X_FRAMES = 8

# MongoDB Connection Details
MONGODB_CONNECTION_STRING = "mongodb://localhost"
DATABASE_NAME = "_workload_"
USERS_COLL_NAME = "users"
MSGS_COLL_NAME = "messages"

# Simulate Time Progression
DEBUG = False
TIME_FRAME_DURATION = 5 if DEBUG else 5 * 60  # 5 minutes
TOTAL_DURATION = 5 * 60 if DEBUG else 3 * 60 * 60  # 3 hour

# Initialize Faker
fake = Faker()

# MongoDB Connection
client = MongoClient(MONGODB_CONNECTION_STRING, server_api=ServerApi('1'))
db = client.get_database(DATABASE_NAME)
users_coll = db.get_collection(USERS_COLL_NAME)
messages_coll = db.get_collection(MSGS_COLL_NAME)


# Returns True 50% of the time
def fifty_fifty():
    return random() < .5


# Returns a random number between 1 and x included
def rand_x(x):
    return randint(1, x)


# Generate a list of {NB_USERS} random users
def random_users():
    docs = []
    for _id in range(1, NB_USERS + 1):
        doc = {
            '_id': _id,
            'firstname': fake.first_name(),
            'lastname': fake.last_name(),
            'phone': fake.phone_number(),
            'age': fake.pyint(min_value=1, max_value=100),
            'ssn': fake.ssn(),
            'address': {
                'number': fake.building_number(),
                'street': fake.street_name(),
                'city': fake.city(),
                'postcode': fake.postcode(),
                'country': fake.country()
            }
        }
        docs.append(doc)
    return docs


# Generates a list of 1000 random messages
def random_messages():
    docs = []
    for _ in range(1000):
        doc = {
            'user_id': fake.pyint(min_value=1, max_value=NB_USERS + 1),
            'message': fake.sentence(nb_words=50),
            'date': datetime.strptime(fake.iso8601(), "%Y-%m-%dT%H:%M:%S")
        }
        docs.append(doc)
    return docs


# Inserts random users
def insert_users():
    users_coll.insert_many(random_users())


# Inserts random messages
def insert_messages():
    messages_coll.insert_many(random_messages())


# Creates mandatory index (or the cluster will explode)
def create_indexes():
    users_coll.create_index('age')
    users_coll.create_index('address.country')
    messages_coll.create_index([('user_id', ASCENDING), ('date', ASCENDING)])
    messages_coll.create_index('date')


# Deletes random messages
def delete_messages():
    if fifty_fifty():
        messages_coll.delete_many({'user_id': fake.pyint(min_value=1, max_value=NB_USERS + 1)})
    else:
        min_date = datetime.strptime(fake.iso8601(), "%Y-%m-%dT%H:%M:%S")
        max_date = min_date + timedelta(days=50)
        messages_coll.delete_many({'date': {'$gte': min_date, '$lte': max_date}})


# Updates random messages
def update_messages():
    x = rand_x(3)
    if x == 1:
        messages_coll.update_many({'user_id': fake.pyint(min_value=1, max_value=NB_USERS + 1)},
                                  {'$set': {'message': fake.sentence(nb_words=50)}, '$inc': {'updated': 1}})
    elif x == 2:
        min_date = datetime.strptime(fake.iso8601(), "%Y-%m-%dT%H:%M:%S")
        max_date = min_date + timedelta(days=50)
        messages_coll.update_many({'date': {'$gte': min_date, '$lte': max_date}},
                                  {'$set': {'message': fake.sentence(nb_words=50)}, '$inc': {'updated': 1}})
    elif x == 3:
        min_date = datetime.strptime(fake.iso8601(), "%Y-%m-%dT%H:%M:%S")
        max_date = min_date + timedelta(days=30)
        messages_coll.find_one_and_update({'user_id': {'$in': [rand_x(NB_USERS), rand_x(NB_USERS), rand_x(NB_USERS)]}, 'date': {'$gte': min_date, '$lte': max_date}},
                                          {'$set': {'message': fake.sentence(nb_words=50)}, '$inc': {'updated': 1}})


# Reads some messages using different queries
def read_messages():
    x = rand_x(4)
    if x == 1:
        list(messages_coll.find({'user_id': fake.pyint(min_value=1, max_value=NB_USERS + 1)}).limit(50))
    elif x == 2:
        list(users_coll.find({'age': rand_x(100)}).limit(50))
    elif x == 3:
        list(users_coll.aggregate([
            {
                '$match': {
                    'age': {
                        '$gte': 30,
                        '$lt': 40
                    }
                }
            }, {
                '$lookup': {
                    'from': 'messages',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'messages'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'firstname': 1,
                    'lastname': 1,
                    'age': 1,
                    'messages': {
                        '$slice': [
                            '$messages', 5
                        ]
                    }
                }
            }, {
                '$sort': {
                    'age': 1
                }
            }, {
                '$limit': 10
            }
        ]))
    elif x == 4:
        list(users_coll.aggregate([
            {
                '$match': {
                    'address.country': fake.country()
                }
            }, {
                '$lookup': {
                    'from': 'messages',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'pipeline': [
                        {
                            '$count': 'count'
                        }
                    ],
                    'as': 'nb_messages'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'firstname': 1,
                    'lastname': 1,
                    'age': 1,
                    'nb_messages': {
                        '$arrayElemAt': [
                            '$nb_messages.count', 0
                        ]
                    }
                }
            }, {
                '$sort': {
                    'age': 1
                }
            }, {
                '$limit': 10
            }
        ]))


# Initialization
def init():
    print(ctime(), "Start reset DB.")
    client.drop_database(DATABASE_NAME)
    create_indexes()
    insert_users()
    print(ctime(), "Done")


if __name__ == "__main__":
    init()
    start_time = int(time())
    stop_time = start_time + TOTAL_DURATION
    operations = [insert_messages, update_messages, read_messages, delete_messages]
    max_size = max(len(op.__name__) for op in operations)
    frames = set()
    frames.add(0)
    while int(time()) < stop_time:
        f_start = time()
        now = int(f_start)
        frame_nb = (now - start_time) // TIME_FRAME_DURATION
        action_nb = frame_nb % len(operations)
        if frame_nb % RESET_DB_EVERY_X_FRAMES == 0 and frame_nb not in frames:
            init()
            frames.add(frame_nb)
        op = operations[action_nb]
        op()
        if DEBUG:
            print('Operation {: >{width}} took {: >10} seconds.'.format(op.__name__, round(time() - f_start, 2), width=str(max_size)))
