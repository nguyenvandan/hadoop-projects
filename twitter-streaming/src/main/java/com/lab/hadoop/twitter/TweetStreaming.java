package com.lab.hadoop.twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TweetStreaming {

	public void getStream(String consumerKey, String consumerSecret,
			String token, String secret) throws Exception {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on
		 * expected TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/**
		 * Declare the host you want to connect to, the endpoint, and
		 * authentication (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("twitter", "api");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
				token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				// optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue))
				.eventMessageQueue(eventQueue); // optional: use this if you
												// want to process client events

		Client hosebirdClient = builder.build();

		// Attempts to establish a connection.
		hosebirdClient.connect();

		// on a different thread, or multiple different threads....
		while (!hosebirdClient.isDone()) {
			String msg = msgQueue.take();
			System.out.print(msg);
		}

		hosebirdClient.stop();
	}

	public static void main(String[] args) throws Exception {
		String consumerKey = "LMXxeA6hzvEQg9LVu8FFNog3V";
		String consumerSecret = "hFv1e3LmFWdcVdfGKVjjDjrRX96yLwnHNqF9WxyxB0QqldqCaW";
		String token = "2364587165-x9OTe5QGHc7nVU3MiEt5XSDnK4LlDU6Qh4bQwvA";
		String secret = "mJ78HOmpzTdmkKswu9XeEGJbTShbazwaYJkl5oOJeADoq";

		TweetStreaming tweetStream = new TweetStreaming();
		tweetStream.getStream(consumerKey, consumerSecret, token, secret);
	}
}
