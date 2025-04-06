using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using lab1.Core.Web;

namespace lab1.Core;

public static class Program
{
	public static async Task produce(ProducerConfig config, string topic, string message, CancellationToken ct=default)
	{

		try
		{
			using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
			{
				DeliveryResult<Null, string> result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message }, ct);
			};
		}
		catch (Exception ex)
		{
			
		}
	}

	public static async Task startConsume(ConsumerConfig config, string[] topics, CancellationToken ct=default)
	{
		using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
		{
			consumer.Subscribe(topics);
			while (!ct.IsCancellationRequested)
			{
				ConsumeResult<Ignore, string> result;
				try
				{
					result = consumer.Consume(ct);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
					continue;
				}
				Console.WriteLine(result.Message.Value);
			}
		}
	}
	
	public static async Task Main(string[] args)
	{
		bool is_using_local_kafka=false;
		string? kafka_bootstrap_addresses = null;
		string? username_sasl= null, password_sasl = null;
		string? path_keystore=null, password_keystore=null;

		if (args.Length < 0 || (args.Length == 1 && (args[0] == "--help" || args[0] == "-help")))
		{
			Console.WriteLine("Please provide [-use_local_kafka] -kafka_bootstrap <host:port> -username_sasl <string> -password_sasl <number>, -keystore_location <path>, -keystore_password <string> arguments.");
			return;
		}
		else
		{
			var throwAndExit = (string argument_name) =>
			{
				Console.WriteLine($"Wrong value provided for argument {argument_name}.");
				Environment.Exit(1);
			};
			for (int i = 0, i_original; i < args.Length; i++)
			{
				try
				{
					i_original = i;
					switch (args[i])
					{
						case "-use_local_kafka":
							is_using_local_kafka=true;
							break;
						case "-kafka_bootstrap":
							kafka_bootstrap_addresses = args[++i];
							break;
						case "-username_sasl":
							username_sasl = args[++i];
							break;
						case "-password_sasl":
							password_sasl = args[++i];
							break;
						case "-keystore_location":
							path_keystore = args[++i];
							break;
						case "-keystore_password":
							password_keystore = args[++i];
							break;
						default:
							break;
					}
				}
				catch (IndexOutOfRangeException)
				{
					string[] args_tmp = new string[args.Length + 4];
					args.CopyTo(args_tmp, 0);
					for (int ii = args.Length; ii < args_tmp.Length; ii++)
						args_tmp[ii] = "-";
					i--;
				}
			}
			if (kafka_bootstrap_addresses == null)
			{
				throwAndExit("-kafka_bootstrap");
			}
			if (username_sasl == null)
			{
				throwAndExit("-username_sasl");
			}
			if (password_sasl == null)
			{
				throwAndExit("-password_sasl");
			}
			if (path_keystore == null)
			{
				throwAndExit("-keystore_location");
			}
			if (password_keystore == null)
			{
				throwAndExit("-keystore_password");
			}
		}
		Console.WriteLine("Program started.");
		
		WebService web_service=new();
		string[] topics=new string[]{"PUBLISH"};
		CancellationTokenSource cts_producer=new(), cts_consumer=new();
		ClientConfig client_config;
		
		if(is_using_local_kafka)
		{
			client_config = new()
			{
				BootstrapServers = kafka_bootstrap_addresses,
			};
		}
		else
		{
			client_config=new()
			{
				BootstrapServers = kafka_bootstrap_addresses,
				SecurityProtocol = SecurityProtocol.SaslSsl,
				SaslMechanism = SaslMechanism.Plain,
				SaslUsername = username_sasl,
				SslKeystoreLocation = path_keystore,
				SslKeystorePassword = password_keystore,
				SaslPassword = password_sasl,
				SslCipherSuites = "DEFAULT:@SECLEVEL=0"
			};
		}
		ProducerConfig producer_config = new (client_config)
		{
			
		};
		ConsumerConfig consumer_config = new (client_config)
		{
			GroupId = "SOLDATOV_VA",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};

		Task.Factory.StartNew(async () =>
		{ 
			await startConsume(consumer_config, topics, cts_consumer.Token);
		}, TaskCreationOptions.LongRunning);

		for (string? input=""; input!="exit" || input!="quit"; )
		{
			Console.WriteLine($"{Environment.NewLine}Input custom message or nothing to generate random message:");
			input = Console.ReadLine();
			if(String.IsNullOrWhiteSpace(input))
			{
				Dictionary<int, (int weight, string type)> _dict_id_request = new () {
					{0, (6, "Анекдот")},
					{1, (3, "Рассказы")},
					{2, (5, "Стишки")},
					{3, (5, "Афоризмы")},
					{4, (5, "Цитаты")},
				};
				string error="к БД.";
				do
				{
					string result_as_json = (await web_service.getAsync($@"http://rzhunemogu.ru/RandJSON.aspx?CType={Helper.GetRandomItem(_dict_id_request, (x) => x.Value.weight).Key}")).Replace("\r\n", "\\n");
					try
					{
						using (JsonDocument doc = JsonDocument.Parse(result_as_json))
						{
							JsonElement root = doc.RootElement;
							input = doc.RootElement.GetProperty("content").GetString();
						}
					}
					catch(Exception ex)
					{
						input=error;
					}
				}
				while(input.Contains(error));
				Console.WriteLine($"Randomly generated message:{Environment.NewLine}{input}");
			}
			await produce(producer_config, topics[0], input, cts_producer.Token);
		}
	}
}

