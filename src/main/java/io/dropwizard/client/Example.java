package io.dropwizard.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.client.Client;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Example {

	private final MetricRegistry metricRegistry = new MetricRegistry();
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	public static void main(String[] args) throws Exception {
		Example me = new Example();
		try {
			me.callWithSafeClient();
			System.out.println("SafeJerseyClientBuilder worked!");
		} catch (Throwable t) {
			System.out.println("SafeJerseyClientBuilder should not fail, but did:");
			System.out.println(t.getMessage());
		}
		try {
			me.callWithStandardClient();
			System.out.println("JerseyClientBuilder should have failed.");
		} catch (Throwable t) {
			System.out.println("JerseyClientBuilder failed as expected:");
			t.printStackTrace();
		}
	}

	private void callWithSafeClient() {
		Client safeClient = new SafeJerseyClientBuilder(metricRegistry).using(objectMapper).using(executor).build("safeClient");
		call(safeClient);
		safeClient.close();
	}

	private void callWithStandardClient() {
		Client standardClient = new JerseyClientBuilder(metricRegistry).using(objectMapper).using(executor).build("standardClient");
		call(standardClient);
		standardClient.close();
	}

	private void call(Client client) {
		client.target("https://www.google.com/").request().get(String.class);
	}
}
