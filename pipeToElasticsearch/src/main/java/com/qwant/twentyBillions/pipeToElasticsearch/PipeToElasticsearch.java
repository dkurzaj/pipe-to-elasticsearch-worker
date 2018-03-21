package com.qwant.twentyBillions.pipeToElasticsearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

public class PipeToElasticsearch extends Countable {
	// Kafka constants
	public static String KAFKA_IP = "localhost";
	public static int KAFKA_PORT = 9092;
	public static String KAFKA_TOPIC;
	public static String KAFKA_GROUP = "test";
		
	// Pipe constants
	public static String NAMED_PIPE_PATH = "/tmp/fifo";

	// Elasticsearch constants
	public static ArrayList<String> ELASTICSEARCH_NODES = new ArrayList<String>();
	public static final String ELASTICSEARCH_PROTOCOL = "http";
	public static String INDEX;
	
	// Default values from the ES BulkProcessor class
	public static int NUM_DOC_PER_BULK_LIMIT = 1000;
	public static long SIZE_MB_PER_BULK_LIMIT = 5L;
	public static long NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE = -1;
	public static int NB_CONCURENT_SENDING_THREADS = 1;
	
	public static boolean VERBOSE = false;
	public static boolean COUNTER = false;
	public static Integer MESSAGE_LIMIT = null;
	public static boolean FAKE_FEEDER = false;
	
	private static final int BEGINING_OF_ID = 29;
	private static final int SIZE_OF_ID = 47;
	
	public static void main(String[] args) {
		// Disable log to avoid error
		StatusLogger.getLogger().setLevel(Level.OFF);
		
		// Args parsing
		PipeToElasticsearch.parseArgs(args);

		// Timer to count messages per second
		Timer timer = new Timer();
		if (COUNTER) {
			timer.schedule(new MessageCounter<PipeToElasticsearch>(), 0, 1000);
		}

		// Elasticsearch initialization
		List<HttpHost> hosts = new ArrayList<HttpHost>();
		for (String hostnameWithPort : ELASTICSEARCH_NODES) {
			String[] ipPort = hostnameWithPort.split(":");
			int port;
			if (ipPort.length == 2) {
				port = Integer.parseInt(ipPort[1]);
			}
			// If no port is specified we set it to the default port 9200
			else {
				port = 9200;
			}
			hosts.add(new HttpHost(ipPort[0], port, ELASTICSEARCH_PROTOCOL));
		}
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts.toArray(new HttpHost[hosts.size()])));

		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				PipeToElasticsearch.bulksInParallel.incrementAndGet();
				int numberOfActions = request.numberOfActions();
				if (VERBOSE) {
					System.out
						.println("[DEBUG] Executing bulk [" + executionId + "] with " + numberOfActions + " requests");
				}
			}
			
			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				PipeToElasticsearch.bulksInParallel.decrementAndGet();
				if (response.hasFailures()) {
					System.out.println("[WARN] Bulk [" + executionId + "] executed with failures: " + response.buildFailureMessage());
				} else {
					System.out.println("[DEBUG] Bulk [" + executionId + "] completed in "
						+ response.getTook().getMillis() + " milliseconds");
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				PipeToElasticsearch.bulksInParallel.decrementAndGet();
				System.err.println("[ERROR] Failed to execute bulk" + failure);
			}
		};

		BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
		// Limit number of documents per bulk (Supposed equivalent in Vespa : FeedParams.maxInFlightRequests)
		builder.setBulkActions(NUM_DOC_PER_BULK_LIMIT);
		// Size of a bulk must reach before being sent (Supposed equivalent in Vespa : FeedParams.maxChunkSizeBytes)
		builder.setBulkSize(new ByteSizeValue(SIZE_MB_PER_BULK_LIMIT, ByteSizeUnit.MB));
		// Limit on time before forcing bulk sending (Supposed equivalent in Vespa : FeedParams.localQueueTimeOut)
		builder.setFlushInterval(TimeValue.timeValueMillis(NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE));
		// Number of parallel bulks possible (Supposed equivalent in Vespa : ConnectionParams.numPersistentConnectionsPerEndpoint)
		builder.setConcurrentRequests(NB_CONCURENT_SENDING_THREADS);
		
		BulkProcessor bulkProcessor = builder.build();
				
		// Get messages from named pipe and send them to Elasticsearch
		long timeSpent = 0;
		try {
			String[] command = new String[] {"mkfifo", NAMED_PIPE_PATH};
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			
			command = new String[] {"/bin/bash", "-c", "kafkacat -b " + KAFKA_IP + ":" + KAFKA_PORT + " -G " + KAFKA_GROUP + " " + KAFKA_TOPIC + " > " + NAMED_PIPE_PATH};
			process = Runtime.getRuntime().exec(command);

			// Connect to the named pipe
			BufferedReader pipe = new BufferedReader(new FileReader(NAMED_PIPE_PATH));
			String message;

			int i = 0;
			long beginTime = System.nanoTime();
			while ((message = pipe.readLine()) != null) {
				if (MESSAGE_LIMIT != null) {
					if (i >= MESSAGE_LIMIT) {
						break;
					}
				}
				PipeToElasticsearch.nbMessagesSinceLastSecond.incrementAndGet();

				while (PipeToElasticsearch.bulksInParallel.get() >= 200) {
					Thread.sleep(10);
				}
				if (!FAKE_FEEDER) {
					String id = message.substring(BEGINING_OF_ID, BEGINING_OF_ID + SIZE_OF_ID);
					bulkProcessor.add(new IndexRequest(INDEX, "doc", id).source(message, XContentType.JSON));
				}

				if (VERBOSE && i % 50000 == 0) {
					 System.out.println(message);
				}
			}
			long endTime = System.nanoTime();
			timeSpent = endTime - beginTime;
			// Leave time in the end to execute the bulk
			Thread.sleep(180000L + 5000L);
			pipe.close();
			client.close();
		} catch (Exception e) {
			System.out.println("[ERROR]: " + e);
			e.printStackTrace();
		} finally {
			bulkProcessor.close();
			timer.cancel();
			timer.purge();
			System.out.println("-------------------------------------------------");
			System.out.println("Total sent: " + PipeToElasticsearch.nbMessagesTotal);
			System.out.println("Time spent: " + TimeUnit.NANOSECONDS.toSeconds(timeSpent) + "s");
			System.out.println("Feed rate: " + PipeToElasticsearch.nbMessagesTotal.get() / TimeUnit.NANOSECONDS.toSeconds(timeSpent) + "messages/s");
		}

	}
	
	private static void parseArgs(String[] args) {
		Options options = new Options();

		Option namedPipePath = new Option("p", "named_pipe_path", true, "Path to the named pipe to get the data from (default: /tmp/fifo)");
		namedPipePath.setRequired(false);
		options.addOption(namedPipePath);

		Option elasticsearchNodes = Option.builder("en").longOpt("elasticsearch_nodes").desc(
				"Elasticsearch nodes <IP>[:<port>]... You can set a list of ES nodes, if you ommit the port of a node, it will be 9200 by default (default: localhost:9200)")
				.hasArgs() // sets that number of arguments is unlimited
				.build();
		elasticsearchNodes.setRequired(false);
		options.addOption(elasticsearchNodes);
		
		Option kafkaIp = new Option("ki", "kafka_ip", true, "Kafka Broker IP (default: localhost)");
		kafkaIp.setRequired(false);
		options.addOption(kafkaIp);

		Option kafkaPort = new Option("kp", "kafka_port", true, "Kafka Broker port (default: 9092)");
		kafkaPort.setRequired(false);
		options.addOption(kafkaPort);
		
		Option kafkaTopic = new Option("t", "topic", true, "Kafka Topic to listen IP");
		kafkaTopic.setRequired(true);
		options.addOption(kafkaTopic);
		
		Option kafkaGroup = new Option("g", "group", true, "Kafka Group this consumer belongs to (default: test)");
		kafkaGroup.setRequired(false);
		options.addOption(kafkaGroup);

		Option index = new Option("i", "index", true, "Elasticsearch Index to send data to");
		index.setRequired(true);
		options.addOption(index);
		
		Option numDocPerBulkLimit = new Option("n", "num_doc_per_bulk_limit", true, "Limit number of documents to send a bulk (default: 1000)");
		numDocPerBulkLimit.setRequired(false);
		options.addOption(numDocPerBulkLimit);
		
		Option sizeMbPerBulkLimit = new Option("s", "size_mb_per_bulk_limit", true, "Limit of size in MB to send a bulk (default: 5)");
		sizeMbPerBulkLimit.setRequired(false);
		options.addOption(sizeMbPerBulkLimit);

		Option nbMsecBeforeFlushingSendingQueue = new Option("f", "nb_msec_before_flushing_sending_queue", true, "Limit of time in ms before flushing the sending queue (default: -1 (disabled))");
		nbMsecBeforeFlushingSendingQueue.setRequired(false);
		options.addOption(nbMsecBeforeFlushingSendingQueue);
		
		Option nbConcurentSendingThreads = new Option("nt", "nb_concurent_sending_threads", true, "Number of concurrent sending threads (default: 1)");
		nbConcurentSendingThreads.setRequired(false);
		options.addOption(nbConcurentSendingThreads);
		
		Option verbose = new Option("v", "verbose", false, "Print one message every 50000 messages transmitted");
		verbose.setRequired(false);
		options.addOption(verbose);
		
		Option counter = new Option("c", "counter", false, "Print the number of messages handled each second");
		counter.setRequired(false);
		options.addOption(counter);
		
		Option message_limit = new Option("l", "message_limit", true, "Set a limit number of messages to send");
		message_limit.setRequired(false);
		options.addOption(message_limit);
		
		Option fakeFeeder = new Option("ff", "fake_feeder", false, "Run the feeder without sending to Elasticsearch");
		fakeFeeder.setRequired(false);
		options.addOption(fakeFeeder);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);

			System.exit(1);
			return;
		}

		if (cmd.getOptionValue("kafka_ip") != null) {
			KAFKA_IP = cmd.getOptionValue("kafka_ip");
		}
		if (cmd.getOptionValue("kafka_port") != null) {
			KAFKA_PORT = Integer.parseInt(cmd.getOptionValue("kafka_port"));
		}
		if (cmd.getOptionValue("topic") != null) {
			KAFKA_TOPIC = cmd.getOptionValue("topic");
		}
		if (cmd.getOptionValue("group") != null) {
			KAFKA_GROUP = cmd.getOptionValue("group");
		}
		if (cmd.getOptionValue("named_pipe_path") != null) {
			NAMED_PIPE_PATH = cmd.getOptionValue("named_pipe_path");
		}
		if (cmd.getOptionValues("elasticsearch_nodes") != null) {
			ELASTICSEARCH_NODES = new ArrayList<String>(Arrays.asList(cmd.getOptionValues("elasticsearch_nodes")));
		}
		else {
			ELASTICSEARCH_NODES.add("localhost:9200");
		}
		if (cmd.getOptionValue("index") != null) {
			INDEX = cmd.getOptionValue("index");
		}
		if (cmd.getOptionValue("num_doc_per_bulk_limit") != null) {
			NUM_DOC_PER_BULK_LIMIT = Integer.parseInt(cmd.getOptionValue("num_doc_per_bulk_limit"));
		}
		if (cmd.getOptionValue("size_mb_per_bulk_limit") != null) {
			SIZE_MB_PER_BULK_LIMIT = Long.parseLong(cmd.getOptionValue("size_mb_per_bulk_limit"));
		}
		if (cmd.getOptionValue("nb_msec_before_flushing_sending_queue") != null) {
			NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE = Long.parseLong(cmd.getOptionValue("nb_msec_before_flushing_sending_queue"));
		}
		if (cmd.getOptionValue("nb_concurent_sending_threads") != null) {
			NB_CONCURENT_SENDING_THREADS = Integer.parseInt(cmd.getOptionValue("nb_concurent_sending_threads"));
		}
		if (cmd.hasOption("verbose")) {
			VERBOSE = true;
		}
		if (cmd.hasOption("counter")) {
			COUNTER = true;
		}
		if (cmd.hasOption("message_limit")) {
			MESSAGE_LIMIT = new Integer(cmd.getOptionValue("message_limit"));
		}
		if (cmd.hasOption("fake_feeder")) {
			FAKE_FEEDER = true;
		}
	}
}
