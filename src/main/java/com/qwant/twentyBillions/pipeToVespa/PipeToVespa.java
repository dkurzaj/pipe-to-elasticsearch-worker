package com.qwant.twentyBillions.pipeToVespa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.yahoo.vespa.http.client.FeedClient;
import com.yahoo.vespa.http.client.FeedClientFactory;
import com.yahoo.vespa.http.client.SimpleLoggerResultCallback;
import com.yahoo.vespa.http.client.config.Cluster;
import com.yahoo.vespa.http.client.config.ConnectionParams;
import com.yahoo.vespa.http.client.config.Endpoint;
import com.yahoo.vespa.http.client.config.FeedParams;
import com.yahoo.vespa.http.client.config.SessionParams;

public class PipeToVespa extends Countable {
	// Kafka constants
	public static String KAFKA_IP = "localhost";
	public static int KAFKA_PORT = 9092;
	public static String KAFKA_TOPIC;
	public static String KAFKA_GROUP = "test";
	
	// Pipe constants
	public static String NAMED_PIPE_PATH = "/tmp/fifo";

	// Vespa constants
	public static ArrayList<String> VESPA_NODES = new ArrayList<String>();
	public static final boolean VESPA_OVER_SSL = false;
	
	// Default values from the Vespa FeedParams and ConnectionParams classes
	public static int NUM_DOC_PER_BULK_LIMIT = 5000;
	public static int SIZE_MB_PER_BULK_LIMIT = 50;
	public static long NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE = 180000L;
	public static int NB_CONCURENT_SENDING_THREADS = 8;
	public static boolean USE_COMPRESSION = false;
	public static int CLIENT_QUEUE_SIZE = 10000;
	public static int THROTTLER_MIN_SIZE = 0;
	

	public static boolean VERBOSE = false;
	public static boolean COUNTER = false;
	public static boolean FAKE_FEEDER = false;

	public static void main(String[] args) {
		// Args parsing
		PipeToVespa.parseArgs(args);

		// Timer to count messages per second
		if (COUNTER) {
			Timer timer = new Timer();
			timer.schedule(new MessageCounter<PipeToVespa>(), 0, 1000);
		}

		// Vespa properties
		Cluster.Builder clusterBuilder = new Cluster.Builder();
		for (String hostnameWithPort : VESPA_NODES) {
			String[] ipPort = hostnameWithPort.split(":");
			int port;
			if (ipPort.length == 2) {
				port = Integer.parseInt(ipPort[1]);
			}
			// If no port is specified we set it to the default port 8080
			else {
				port = 8080;
			}
			clusterBuilder.addEndpoint(Endpoint.create(ipPort[0], port, VESPA_OVER_SSL));
		}
		
		final SessionParams sessionParams = new SessionParams.Builder()
			.addCluster(clusterBuilder.build())
			.setFeedParams(new FeedParams.Builder()
					.setDataFormat(FeedParams.DataFormat.JSON_UTF8)
					// Limit number of documents per chunk (Supposed equivalent in Elasticsearch : BulkProcessor.Builder.bulkActions)
					.setMaxInFlightRequests(NUM_DOC_PER_BULK_LIMIT)
					// Size of a chunk must reach before being sent (Supposed equivalent in Elasticsearch : BulkProcessor.Builder.bulkSize)
					.setMaxChunkSizeBytes(SIZE_MB_PER_BULK_LIMIT * 1024) // KB
					// Limit on time before forcing chunk sending (Supposed equivalent in Elasticsearch : BulkProcessor.Builder.flushInterval)
					.setLocalQueueTimeOut(NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE)
//					.setMaxSleepTimeMs(500)
					.build())
			.setConnectionParams(new ConnectionParams.Builder()
					// Number of parallel chunks possible (Supposed equivalent in Elasticsearch : BulkProcessor.Builder.concurrentRequests)
					.setNumPersistentConnectionsPerEndpoint(NB_CONCURENT_SENDING_THREADS)
					.setUseCompression(USE_COMPRESSION)
					.build())
			.setClientQueueSize(CLIENT_QUEUE_SIZE)
			.setThrottlerMinSize(THROTTLER_MIN_SIZE)
			.build();
		
		
		// Vespa initialization
		final AtomicInteger resultsReceived = new AtomicInteger(0);
		SimpleLoggerResultCallback callback = new SimpleLoggerResultCallback(resultsReceived, 10000);
		FeedClient feedClient = FeedClientFactory.create(sessionParams, callback);

		// Get messages from named pipe and send them to Vespa
		try {
			String[] command = new String[] {"mkfifo", NAMED_PIPE_PATH};
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			
			command = new String[] {"/bin/bash", "-c", "kafkacat -o -1000000 -t " + KAFKA_TOPIC + " -b " + KAFKA_IP + ":" + KAFKA_PORT + " -G " + KAFKA_GROUP + " -C > " + NAMED_PIPE_PATH};
			process = Runtime.getRuntime().exec(command);

			// Connect to the named pipe
			BufferedReader pipe = new BufferedReader(new FileReader(NAMED_PIPE_PATH));
			String message;

			while ((message = pipe.readLine()) != null) {
				PipeToVespa.nbMessagesSinceLastSecond.incrementAndGet();

				// Add the Vespa "put" operation
				// JSONObject jsonObj = new JSONObject(message);
				// String docId = jsonObj.getJSONObject("fields").getString("id");
				// jsonObj.accumulate("put", "id:site-search:site::" + docId);
				// CharSequence docData = jsonObj.toString();

				// Send data to Vespa
				if (!FAKE_FEEDER) {
					feedClient.stream(Double.toString(Math.random()), message);
				}
				
				if (VERBOSE) {
					 System.out.println(message);
				}
			}
			pipe.close();
		} catch (Exception e) {
			System.out.println("[ERROR]: " + e);
			e.printStackTrace();
		} finally {
			feedClient.close();
		}

	}

	private static void parseArgs(String[] args) {
		Options options = new Options();

		Option namedPipePath = new Option("p", "named_pipe_path", true, "Path to the named pipe to get the data from (default: /tmp/fifo)");
		namedPipePath.setRequired(false);
		options.addOption(namedPipePath);

		Option vespaNodes = Option.builder("vn").longOpt("vespa_nodes").desc(
				"Vespa nodes <IP>[:<port>]... You can set a list of Vespa nodes, if you ommit the port of a node, it will be 8080 by default (default: localhost:8080)")
				.hasArgs() // sets that number of arguments is unlimited
				.build();
		vespaNodes.setRequired(false);
		options.addOption(vespaNodes);
		
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

		Option numDocPerBulkLimit = new Option("n", "num_doc_per_bulk_limit", true, "Limit number of documents to send a chunk (default: 5000)");
		numDocPerBulkLimit.setRequired(false);
		options.addOption(numDocPerBulkLimit);
		
		Option sizeMbPerBulkLimit = new Option("s", "size_mb_per_bulk_limit", true, "Limit of size in MB to send a chunk (default: 50KB)");
		sizeMbPerBulkLimit.setRequired(false);
		options.addOption(sizeMbPerBulkLimit);

		Option nbMsecBeforeFlushingSendingQueue = new Option("f", "nb_msec_before_flushing_sending_queue", true, "Limit of time in ms before flushing the sending queue (default: 180000ms)");
		nbMsecBeforeFlushingSendingQueue.setRequired(false);
		options.addOption(nbMsecBeforeFlushingSendingQueue);
		
		Option nbConcurentSendingThreads = new Option("nt", "nb_concurent_sending_threads", true, "Number of concurrent sending threads (default: 8)");
		nbConcurentSendingThreads.setRequired(false);
		options.addOption(nbConcurentSendingThreads);
		
		Option compress = new Option("C", "compress", false, "Compress the message sent in the chunks");
		compress.setRequired(false);
		options.addOption(compress);
		
		Option clientQueueSize = new Option("qs", "client_queue_size", true, "Size of the client queue (default: 10000)");
		clientQueueSize.setRequired(false);
		options.addOption(clientQueueSize);
		
		Option throttlerMinSize = new Option("ts", "throttler_min_size", true, "Minimum value for the throttler (default: 0)");
		throttlerMinSize.setRequired(false);
		options.addOption(throttlerMinSize);

		Option verbose = new Option("v", "verbose", false, "Print messages transmitted");
		verbose.setRequired(false);
		options.addOption(verbose);

		Option counter = new Option("c", "counter", false, "Print the number of messages handled each second");
		counter.setRequired(false);
		options.addOption(counter);
		
		Option fakeFeeder = new Option("ff", "fake_feeder", false, "Run the feeder without sending to Vespa");
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
		if (cmd.getOptionValues("vespa_nodes") != null) {
			VESPA_NODES = new ArrayList<String>(Arrays.asList(cmd.getOptionValues("vespa_nodes")));
		}
		else {
			VESPA_NODES.add("localhost:8080");
		}
		if (cmd.getOptionValue("num_doc_per_bulk_limit") != null) {
			NUM_DOC_PER_BULK_LIMIT = Integer.parseInt(cmd.getOptionValue("num_doc_per_bulk_limit"));
		}
		if (cmd.getOptionValue("size_mb_per_bulk_limit") != null) {
			// Converted to MB
			SIZE_MB_PER_BULK_LIMIT = Integer.parseInt(cmd.getOptionValue("size_mb_per_bulk_limit")) * 1024;
		}
		if (cmd.getOptionValue("nb_msec_before_flushing_sending_queue") != null) {
			NB_MSEC_BEFORE_FLUSHING_SENDING_QUEUE = Long.parseLong(cmd.getOptionValue("nb_msec_before_flushing_sending_queue"));
		}
		if (cmd.getOptionValue("nb_concurent_sending_threads") != null) {
			NB_CONCURENT_SENDING_THREADS = Integer.parseInt(cmd.getOptionValue("nb_concurent_sending_threads"));
		}
		if (cmd.hasOption("compress")) {
			USE_COMPRESSION = true;
		}
		if (cmd.getOptionValue("client_queue_size") != null) {
			CLIENT_QUEUE_SIZE = Integer.parseInt(cmd.getOptionValue("client_queue_size"));
		}
		if (cmd.getOptionValue("throttler_min_size") != null) {
			THROTTLER_MIN_SIZE = Integer.parseInt(cmd.getOptionValue("throttler_min_size"));
		}
		if (cmd.hasOption("verbose")) {
			VERBOSE = true;
		}
		if (cmd.hasOption("counter")) {
			COUNTER = true;
		}
		if (cmd.hasOption("fake_feeder")) {
			FAKE_FEEDER = true;
		}
	}

}
