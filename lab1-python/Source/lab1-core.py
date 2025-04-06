import sys
import json
import requests
from confluent_kafka import Producer, Consumer, KafkaException
import random
import threading

def produce(config, topic, message):
    try:
        def log_handler(logger, level, fac, buf):
            if "Configuration property" not in buf:
                print(buf)

        producer = Producer(config, logger=log_handler)
        producer.produce(topic, value=message)
        producer.flush()
        print("Message sent.")
    except Exception as ex:
        print(f"Error in producer: {ex}")

def startConsume(config, topics):
    consumer = Consumer(config)
    consumer.subscribe(topics)
    
    try:
        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    print(f"Consumed: {msg.value()}")
            except Exception as ex:
                print(f"Error in consumer: {ex}")
                continue
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def getRandomText():
    types = {
        0: (6, "Анекдот"),
        1: (3, "Рассказы"),
        2: (5, "Стишки"),
        3: (5, "Афоризмы"),
        4: (5, "Цитаты")
    }
    
    choices = []
    for k, v in types.items():
        choices.extend([k] * v[0])
    selected = random.choice(choices)
    
    error = "к БД."
    while True:
        try:
            response = requests.get(f"http://rzhunemogu.ru/RandJSON.aspx?CType={selected}")
            result_json = response.text.replace("\r\n", "\\n")
            data = json.loads(result_json)
            content = data["content"]
            if error in content:
                continue
            return content
        except (json.JSONDecodeError, KeyError, requests.RequestException):
            continue

if __name__ == "__main__":
    is_using_local_kafka = False
    kafka_bootstrap_addresses = None
    username_sasl = None
    password_sasl = None
    path_keystore = None
    password_keystore = None

    if len(sys.argv) < 2 or (len(sys.argv) == 2 and sys.argv[1] in ["--help", "-help"]):
        print("Please provide [-use_local_kafka] -kafka_bootstrap <host:port> -username_sasl <string> -password_sasl <number>, -keystore_location <path>, -keystore_password <string> arguments.")
        exit(1)
    
    i = 1
    while i < len(sys.argv):
        try:
            arg = sys.argv[i]
            if arg == "-use_local_kafka":
                is_using_local_kafka = True
            elif arg == "-kafka_bootstrap":
                kafka_bootstrap_addresses = sys.argv[i+1]
                i += 1
            elif arg == "-username_sasl":
                username_sasl = sys.argv[i+1]
                i += 1
            elif arg == "-password_sasl":
                password_sasl = sys.argv[i+1]
                i += 1
            elif arg == "-keystore_location":
                path_keystore = sys.argv[i+1]
                i += 1
            elif arg == "-keystore_password":
                password_keystore = sys.argv[i+1]
                i += 1
            i += 1
        except IndexError:
            print(f"Missing value for argument {sys.argv[i]}")
            sys.exit(1)

    if not kafka_bootstrap_addresses:
        print("Missing required argument: -kafka_bootstrap")
        sys.exit(1)
    if not username_sasl and not is_using_local_kafka:
        print("Missing required argument: -username_sasl")
        sys.exit(1)
    if not password_sasl and not is_using_local_kafka:
        print("Missing required argument: -password_sasl")
        sys.exit(1)
    if not path_keystore and not is_using_local_kafka:
        print("Missing required argument: -keystore_location")
        sys.exit(1)
    if not password_keystore and not is_using_local_kafka:
        print("Missing required argument: -keystore_password")
        sys.exit(1)

    print("Program started.")

    topics = ["PUBLISH"]
    
    if is_using_local_kafka:
        client_config = {
            'bootstrap.servers': kafka_bootstrap_addresses,
        }
    else:
        client_config = {
            'bootstrap.servers': kafka_bootstrap_addresses,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username_sasl,
            'ssl.keystore.location': path_keystore,
            'ssl.keystore.password': password_keystore,
            'sasl.password': password_sasl,
            'ssl.cipher.suites': 'DEFAULT:@SECLEVEL=0'
        }

    producer_config = client_config.copy()
    consumer_config = client_config.copy()
    consumer_config.update({
        'group.id': 'SOLDATOV_VA',
        'auto.offset.reset': 'earliest'
    })

    consumer_thread = threading.Thread(
        target=startConsume, 
        args=(consumer_config, topics),
        daemon=True
    )
    consumer_thread.start()

    while True:
        print("\nInput custom message or nothing to generate random message (type 'exit' or 'quit' to stop):")
        user_input = input().strip()
        
        if user_input.lower() in ["exit", "quit"]:
            break
            
        if not user_input:
            user_input = getRandomText()
            print(f"Randomly generated message:\n{user_input}")
        
        produce(producer_config, topics[0], user_input)
