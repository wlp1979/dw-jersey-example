package io.dropwizard.client;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.validation.Validator;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.RxInvokerProvider;
import javax.ws.rs.core.Configuration;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.config.Registry;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.spi.ConnectorProvider;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.httpclient.HttpClientMetricNameStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jersey.gzip.ConfiguredGZipEncoder;
import io.dropwizard.jersey.gzip.GZipDecoder;
import io.dropwizard.jersey.jackson.JacksonFeature;
import io.dropwizard.jersey.validation.HibernateValidationBinder;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;

/**
 * This is a copy of {@link io.dropwizard.client.JerseyClientBuilder} however it explicitly uses {@link org.glassfish.jersey.client.JerseyClientBuilder}
 * to construct the client object so it avoids class loader issues with other java rx implementations in the classpath.
 */
public class SafeJerseyClientBuilder {

	private final List<Object> singletons = new ArrayList<>();
	private final List<Class<?>> providers = new ArrayList<>();
	private final Map<String, Object> properties = new LinkedHashMap<>();
	private JerseyClientConfiguration configuration = new JerseyClientConfiguration();

	private HttpClientBuilder apacheHttpClientBuilder;
	private Validator validator = Validators.newValidator();

	@Nullable
	private Environment environment;

	@Nullable
	private ObjectMapper objectMapper;

	@Nullable
	private ExecutorService executorService;

	@Nullable
	private ConnectorProvider connectorProvider;

	public SafeJerseyClientBuilder(Environment environment) {
		this.apacheHttpClientBuilder = new HttpClientBuilder(environment);
		this.environment = environment;
	}

	public SafeJerseyClientBuilder(MetricRegistry metricRegistry) {
		this.apacheHttpClientBuilder = new HttpClientBuilder(metricRegistry);
	}

	public void setApacheHttpClientBuilder(HttpClientBuilder apacheHttpClientBuilder) {
		this.apacheHttpClientBuilder = apacheHttpClientBuilder;
	}

	/**
	 * Adds the given object as a Jersey provider.
	 *
	 * @param provider a Jersey provider
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder withProvider(Object provider) {
		singletons.add(requireNonNull(provider));
		return this;
	}

	/**
	 * Adds the given class as a Jersey provider. <p/><b>N.B.:</b> This class must either have a
	 * no-args constructor or use Jersey's built-in dependency injection.
	 *
	 * @param klass a Jersey provider class
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder withProvider(Class<?> klass) {
		providers.add(requireNonNull(klass));
		return this;
	}

	/**
	 * Sets the state of the given Jersey property.
	 * <p/>
	 * <p/><b>WARNING:</b> The default connector ignores Jersey properties.
	 * Use {@link JerseyClientConfiguration} instead.
	 *
	 * @param propertyName  the name of the Jersey property
	 * @param propertyValue the state of the Jersey property
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder withProperty(String propertyName, Object propertyValue) {
		properties.put(propertyName, propertyValue);
		return this;
	}

	/**
	 * Uses the given {@link JerseyClientConfiguration}.
	 *
	 * @param configuration a configuration object
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(JerseyClientConfiguration configuration) {
		this.configuration = configuration;
		apacheHttpClientBuilder.using(configuration);
		return this;
	}

	/**
	 * Uses the given {@link Environment}.
	 *
	 * @param environment a Dropwizard {@link Environment}
	 * @return {@code this}
	 * @see #using(java.util.concurrent.ExecutorService, com.fasterxml.jackson.databind.ObjectMapper)
	 */
	public SafeJerseyClientBuilder using(Environment environment) {
		this.environment = environment;
		return this;
	}

	/**
	 * Use the given {@link Validator} instance.
	 *
	 * @param validator a {@link Validator} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(Validator validator) {
		this.validator = validator;
		return this;
	}

	/**
	 * Uses the given {@link ExecutorService} and {@link ObjectMapper}.
	 *
	 * @param executorService a thread pool
	 * @param objectMapper    an object mapper
	 * @return {@code this}
	 * @see #using(io.dropwizard.setup.Environment)
	 */
	public SafeJerseyClientBuilder using(ExecutorService executorService, ObjectMapper objectMapper) {
		this.executorService = executorService;
		this.objectMapper = objectMapper;
		return this;
	}

	/**
	 * Uses the given {@link ExecutorService}.
	 *
	 * @param executorService a thread pool
	 * @return {@code this}
	 * @see #using(io.dropwizard.setup.Environment)
	 */
	public SafeJerseyClientBuilder using(ExecutorService executorService) {
		this.executorService = executorService;
		return this;
	}

	/**
	 * Uses the given {@link ObjectMapper}.
	 *
	 * @param objectMapper    an object mapper
	 * @return {@code this}
	 * @see #using(io.dropwizard.setup.Environment)
	 */
	public SafeJerseyClientBuilder using(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		return this;
	}

	/**
	 * Use the given {@link ConnectorProvider} instance.
	 * <p/><b>WARNING:</b> Use it with a caution. Most of features will not
	 * work in a custom connection provider.
	 *
	 * @param connectorProvider a {@link ConnectorProvider} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(ConnectorProvider connectorProvider) {
		this.connectorProvider = connectorProvider;
		return this;
	}

	/**
	 * Uses the {@link org.apache.http.client.HttpRequestRetryHandler} for handling request retries.
	 *
	 * @param httpRequestRetryHandler a HttpRequestRetryHandler
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(HttpRequestRetryHandler httpRequestRetryHandler) {
		apacheHttpClientBuilder.using(httpRequestRetryHandler);
		return this;
	}

	/**
	 * Use the given {@link DnsResolver} instance.
	 *
	 * @param resolver a {@link DnsResolver} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(DnsResolver resolver) {
		apacheHttpClientBuilder.using(resolver);
		return this;
	}

	/**
	 * Use the given {@link HostnameVerifier} instance.
	 *
	 * Note that if {@link io.dropwizard.client.ssl.TlsConfiguration#isVerifyHostname()}
	 * returns false, all host name verification is bypassed, including
	 * host name verification performed by a verifier specified
	 * through this interface.
	 *
	 * @param verifier a {@link HostnameVerifier} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(HostnameVerifier verifier) {
		apacheHttpClientBuilder.using(verifier);
		return this;
	}

	/**
	 * Use the given {@link Registry} instance of connection socket factories.
	 *
	 * @param registry a {@link Registry} instance of connection socket factories
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(Registry<ConnectionSocketFactory> registry) {
		apacheHttpClientBuilder.using(registry);
		return this;
	}

	/**
	 * Use the given {@link HttpClientMetricNameStrategy} instance.
	 *
	 * @param metricNameStrategy a {@link HttpClientMetricNameStrategy} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(HttpClientMetricNameStrategy metricNameStrategy) {
		apacheHttpClientBuilder.using(metricNameStrategy);
		return this;
	}

	/**
	 * Use the given environment name. This is used in the user agent.
	 *
	 * @param environmentName an environment name to use in the user agent.
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder name(String environmentName) {
		apacheHttpClientBuilder.name(environmentName);
		return this;
	}

	/**
	 * Use the given {@link HttpRoutePlanner} instance.
	 *
	 * @param routePlanner a {@link HttpRoutePlanner} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(HttpRoutePlanner routePlanner) {
		apacheHttpClientBuilder.using(routePlanner);
		return this;
	}

	/**
	 * Use the given {@link CredentialsProvider} instance.
	 *
	 * @param credentialsProvider a {@link CredentialsProvider} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(CredentialsProvider credentialsProvider) {
		apacheHttpClientBuilder.using(credentialsProvider);
		return this;
	}

	/**
	 * Use the given {@link ServiceUnavailableRetryStrategy} instance.
	 *
	 * @param serviceUnavailableRetryStrategy a {@link ServiceUnavailableRetryStrategy} instance
	 * @return {@code this}
	 */
	public SafeJerseyClientBuilder using(ServiceUnavailableRetryStrategy serviceUnavailableRetryStrategy) {
		apacheHttpClientBuilder.using(serviceUnavailableRetryStrategy);
		return this;
	}

	/**
	 * Builds the {@link Client} instance with a custom reactive client provider.
	 *
	 * @return a fully-configured {@link Client}
	 */
	public <RX extends RxInvokerProvider<?>> Client buildRx(String name, Class<RX> invokerType) {
		return build(name).register(invokerType);
	}

	/**
	 * Builds the {@link Client} instance.
	 *
	 * @return a fully-configured {@link Client}
	 */
	public JerseyClient build(String name) {
		if ((environment == null) && ((executorService == null) || (objectMapper == null))) {
			throw new IllegalStateException("Must have either an environment or both " +
					"an executor service and an object mapper");
		}

		if (executorService == null) {
			// Create an ExecutorService based on the provided
			// configuration. The DisposableExecutorService decorator
			// is used to ensure that the service is shut down if the
			// Jersey client disposes of it.
			executorService = requireNonNull(environment).lifecycle()
					.executorService("jersey-client-" + name + "-%d")
					.minThreads(configuration.getMinThreads())
					.maxThreads(configuration.getMaxThreads())
					.workQueue(new ArrayBlockingQueue<>(configuration.getWorkQueueSize()))
					.build();
		}

		if (objectMapper == null) {
			objectMapper = requireNonNull(environment).getObjectMapper();
		}

		if (environment != null) {
			validator = environment.getValidator();
		}

		return build(name, executorService, objectMapper, validator);
	}

	private JerseyClient build(String name, ExecutorService threadPool,
							   ObjectMapper objectMapper,
							   Validator validator) {
		if (!configuration.isGzipEnabled()) {
			apacheHttpClientBuilder.disableContentCompression(true);
		}
		// This is the key change from the dropwizard JerseyClientBuilder. That builder used the java rx client builder mechanism.
		final JerseyClient client = JerseyClientBuilder.createClient(buildConfig(name, threadPool, objectMapper, validator));
		client.register(new JerseyIgnoreRequestUserAgentHeaderFilter());

		// Tie the client to server lifecycle
		if (environment != null) {
			environment.lifecycle().manage(new Managed() {
				@Override
				public void start() throws Exception {
				}

				@Override
				public void stop() throws Exception {
					client.close();
				}
			});
		}
		if (configuration.isGzipEnabled()) {
			client.register(new GZipDecoder());
			client.register(new ConfiguredGZipEncoder(configuration.isGzipEnabledForRequests()));
		}

		return client;
	}

	private Configuration buildConfig(final String name, final ExecutorService threadPool,
									  final ObjectMapper objectMapper,
									  final Validator validator) {
		final ClientConfig config = new ClientConfig();

		for (Object singleton : this.singletons) {
			config.register(singleton);
		}

		for (Class<?> provider : this.providers) {
			config.register(provider);
		}

		config.register(new JacksonFeature(objectMapper));
		config.register(new HibernateValidationBinder(validator));

		for (Map.Entry<String, Object> property : this.properties.entrySet()) {
			config.property(property.getKey(), property.getValue());
		}

		config.register(new DropwizardExecutorProvider(threadPool));

		if (connectorProvider == null) {
			final ConfiguredCloseableHttpClient apacheHttpClient =
					apacheHttpClientBuilder.buildWithDefaultRequestConfiguration(name);
			config.connectorProvider((client, runtimeConfig) -> createDropwizardApacheConnector(apacheHttpClient));
		} else {
			config.connectorProvider(connectorProvider);
		}

		return config;
	}

	/**
	 * Builds {@link DropwizardApacheConnector} based on the configured Apache HTTP client
	 * as {@link ConfiguredCloseableHttpClient} and the chunked encoding configuration set by the user.
	 */
	protected DropwizardApacheConnector createDropwizardApacheConnector(ConfiguredCloseableHttpClient configuredClient) {
		return new DropwizardApacheConnector(configuredClient.getClient(), configuredClient.getDefaultRequestConfig(),
				configuration.isChunkedEncodingEnabled());
	}
}
