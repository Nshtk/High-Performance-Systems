import sys
import json
import requests
from confluent_kafka import Producer, Consumer, KafkaException

def produce(config, topic, message):
	try:
		producer = Producer(config)
		producer.produce(topic, value=message)
		producer.flush()
	except Exception as ex:
		pass

def start_consume(config, topics):
	consumer = Consumer(config)
	consumer.subscribe(topics)
	
	try:
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				continue
			if msg.error():
				raise KafkaException(msg.error())
			else:
				print(msg.value())
	except KeyboardInterrupt:
		pass
	finally:
		consumer.close()

def main():
	kafka_bootstrap_addresses = None
	username_sasl = None
	password_sasl = None

	if len(sys.argv) < 2 or (len(sys.argv) == 2 and sys.argv[1] in ["--help", "-help"]):
		print("Please provide -password_sasl <number> and ip_type [v4|v6] arguments!")
		return
	else:
		def throw_and_exit(argument_name):
			print(f"Wrong value provided for argument {argument_name}.")
			sys.exit(1)
			
		i = 1
		while i < len(sys.argv):
			try:
				arg = sys.argv[i]
				if arg == "-kafka_bootstrap":
					i += 1
					kafka_bootstrap_addresses = sys.argv[i]
				elif arg == "-username_sasl":
					i += 1
					username_sasl = sys.argv[i]
				elif arg == "-password_sasl":
					i += 1
					password_sasl = sys.argv[i]
				i += 1
			except IndexError:
				i -= 1
				continue
		
		if password_sasl is None:
			throw_and_exit("-password_sasl")

	print("Program started.")

	topics = ["PUBLISH"]
	
	client_config = {
		'bootstrap.servers': kafka_bootstrap_addresses,
		'security.protocol': 'SASL_SSL',
		'sasl.mechanism': 'PLAIN',
		'sasl.username': username_sasl,
		'ssl.keystore.location': 'Data/keystore_reg.pfx',
		#'ssl.keystore.type': 'PKCS12',
		'ssl.keystore.password': '12345678',
		'sasl.password': password_sasl,
		'ssl.cipher.suites': 'DEFAULT:@SECLEVEL=0'
	}
	
	producer_config = client_config.copy()
	
	consumer_config = client_config.copy()
	consumer_config.update({
		'group.id': 'SOLDATOV_VA',
		'auto.offset.reset': 'earliest'
	})

	# Test producer
	producer = Producer(producer_config)
	producer.produce(topics[0], value="dsa")
	producer.flush()

	# Start consumer in a separate thread
	import threading
	consumer_thread = threading.Thread(target=start_consume, args=(consumer_config, topics))
	consumer_thread.daemon = True
	consumer_thread.start()

	while True:
		print("\nInput custom message or nothing to generate random message:")
		user_input = input().strip()
		
		if not user_input:
			dict_id_request = {
				0: (6, "Анекдот"),
				1: (3, "Рассказы"),
				2: (5, "Стишки"),
				3: (5, "Афоризмы"),
				4: (5, "Цитаты")
			}
			
			# Simple weighted random selection
			import random
			choices = []
			for k, v in dict_id_request.items():
				choices.extend([k] * v[0])
			selected = random.choice(choices)
			
			error = "к БД."
			while True:
				try:
					response = requests.get(f"http://rzhunemogu.ru/RandJSON.aspx?CType={selected}")
					result_json = response.text.replace("\r\n", "\\n")
					data = json.loads(result_json)
					user_input = data["content"]
					break
				except (json.JSONDecodeError, KeyError):
					user_input = error
					continue
			
			print(f"Randomly generated message:\n{user_input}")
		
		if user_input.lower() in ["exit", "quit"]:
			break
			
		produce(producer_config, topics[0], user_input)

if __name__ == "__main__":
	main()