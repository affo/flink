package org.apache.flink.queryablestate.server;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.GlobalStateClient;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.FutureUtils;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceTypeInfo;
import org.apache.flink.queryablestate.client.state.ImmutableStateBinder;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.Client;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by affo on 03/11/17.
 * <p>
 * This gateway is an encapsulation of the logic previously contained in the ProxyHandler.
 * This class was created because it has to be possible to instantiate a gateway from an operator.
 */
public class GlobalStateGateway implements GlobalStateClient {
	private static final Logger LOG = LoggerFactory.getLogger(GlobalStateGateway.class);

	/**
	 * The proxy using this handler.
	 */
	private final KvStateClientProxy proxy;

	private final ExecutorService queryExecutor;

	private final String serverName;

	/**
	 * Network client to forward queries to {@link KvStateServerImpl state server}
	 * instances inside the cluster.
	 */
	private final Client<KvStateInternalRequest, KvStateResponse> kvStateClient;

	/**
	 * A cache to hold the location of different states for which we have already seen requests.
	 */
	private final ConcurrentMap<Tuple2<JobID, String>, CompletableFuture<KvStateLocation>> lookupCache =
		new ConcurrentHashMap<>();

	private final Map<InetSocketAddress, QueryableStateClient> queryableStateClientCache = new ConcurrentHashMap<>();

	/**
	 * Could be random in the case it is not used by operators
	 */
	private final JobID jobID;
	private final ExecutionConfig executionConfig;

	/**
	 * Directly instantiated by {@link org.apache.flink.queryablestate.client.proxy.KvStateClientProxyHandler}.
	 *
	 * @param proxy
	 * @param queryExecutor
	 * @param serverName
	 * @param numQueryThreads
	 */
	public GlobalStateGateway(
		KvStateClientProxy proxy,
		ExecutorService queryExecutor,
		String serverName,
		int numQueryThreads) {
		this.proxy = proxy;
		this.queryExecutor = queryExecutor;
		this.serverName = serverName;
		this.kvStateClient = createInternalClient(numQueryThreads);
		this.jobID = null;
		this.executionConfig = null;
	}

	/**
	 * Instantiated using reflection (breaking dependency cycle?) by the
	 * {@link org.apache.flink.runtime.query.QueryableStateUtils#createGlobalStateClient(ExecutionConfig, JobID, KvStateClientProxy, int)
	 * org.apache.flink.runtime.query.QueryableStateUtils#createGlobalStateClient} method.
	 * <p>
	 * Used for programmatic requests inside of operators.
	 *
	 * @param proxy
	 * @param numQueryThreads
	 */
	public GlobalStateGateway(ExecutionConfig executionConfig, JobID jobID, KvStateClientProxy proxy, int numQueryThreads) {
		this.proxy = proxy;
		this.queryExecutor = createQueryExecutor(numQueryThreads);
		this.serverName = "Global State Client";
		this.kvStateClient = createInternalClient(numQueryThreads);
		this.jobID = jobID;
		this.executionConfig = executionConfig;
	}

	/**
	 * Creates a thread pool for the query execution.
	 *
	 * @return Thread pool for query execution
	 */
	private ExecutorService createQueryExecutor(int numQueryThreads) {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
			.setDaemon(true)
			.setNameFormat("Flink " + serverName + " Thread %d")
			.build();
		return Executors.newFixedThreadPool(numQueryThreads, threadFactory);
	}

	private static Client<KvStateInternalRequest, KvStateResponse> createInternalClient(int threads) {
		final MessageSerializer<KvStateInternalRequest, KvStateResponse> messageSerializer =
			new MessageSerializer<>(
				new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
				new KvStateResponse.KvStateResponseDeserializer());

		return new Client<>(
			"Queryable State Proxy Client",
			threads,
			messageSerializer,
			new DisabledKvStateRequestStats());
	}

	public CompletableFuture<KvStateResponse> getState(
		final KvStateRequest request,
		final boolean forceUpdate) {

		return getKvStateLookupInfo(request.getJobId(), request.getStateName(), forceUpdate)
			.thenComposeAsync((Function<KvStateLocation, CompletableFuture<KvStateResponse>>) location -> {
				final int keyGroupIndex = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
					request.getKeyHashCode(), location.getNumKeyGroups());

				final InetSocketAddress serverAddress = location.getKvStateServerAddress(keyGroupIndex);
				if (serverAddress == null) {
					return FutureUtils.getFailedFuture(new UnknownKvStateKeyGroupLocationException(serverName));
				} else {
					// Query server
					final KvStateID kvStateId = location.getKvStateID(keyGroupIndex);
					final KvStateInternalRequest internalRequest = new KvStateInternalRequest(
						kvStateId, request.getSerializedKeyAndNamespace());
					return kvStateClient.sendRequest(serverAddress, internalRequest);
				}
			}, queryExecutor);
	}

	/**
	 * Lookup the {@link KvStateLocation} for the given job and queryable state name.
	 * <p>
	 * <p>The job manager will be queried for the location only if forced or no
	 * cached location can be found. There are no guarantees about
	 *
	 * @param jobId              JobID the state instance belongs to.
	 * @param queryableStateName Name under which the state instance has been published.
	 * @param forceUpdate        Flag to indicate whether to force a update via the lookup service.
	 * @return Future holding the KvStateLocation
	 */
	private CompletableFuture<KvStateLocation> getKvStateLookupInfo(
		final JobID jobId,
		final String queryableStateName,
		final boolean forceUpdate) {

		final Tuple2<JobID, String> cacheKey = new Tuple2<>(jobId, queryableStateName);
		final CompletableFuture<KvStateLocation> cachedFuture = lookupCache.get(cacheKey);

		if (!forceUpdate && cachedFuture != null && !cachedFuture.isCompletedExceptionally()) {
			LOG.debug("Retrieving location for state={} of job={} from the cache.", jobId, queryableStateName);
			return cachedFuture;
		}

		LOG.debug("Retrieving location for state={} of job={} from the job manager.", jobId, queryableStateName);

		return proxy.getJobManagerFuture().thenComposeAsync(
			jobManagerGateway -> {
				final Object msg = new KvStateMessage.LookupKvStateLocation(jobId, queryableStateName);
				final CompletableFuture<KvStateLocation> locationFuture = org.apache.flink.runtime.concurrent.FutureUtils.toJava(
					jobManagerGateway.ask(msg, FiniteDuration.apply(1000L, TimeUnit.MILLISECONDS))
						.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class)));

				lookupCache.put(cacheKey, locationFuture);
				return locationFuture;
			}, queryExecutor);
	}

	public void shutdown() {
		kvStateClient.shutdown();
	}

	//-------------------------- For Operators

	@Override
	public <K, T> T get(ValueStateDescriptor<T> stateDescriptor, TypeInformation<K> keyType, K key) throws Exception {
		// TODO should wrap this part in an InternalQueryableStateClient
		TypeSerializer<K> keySerializer = keyType.createSerializer(executionConfig);
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceTypeInfo.INSTANCE
			.createSerializer(executionConfig);

		stateDescriptor.initializeSerializerUnlessSet(executionConfig);

		final byte[] serializedKeyAndNamespace;
		try {
			serializedKeyAndNamespace = KvStateSerializer
				.serializeKeyAndNamespace(key, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer);
		} catch (IOException e) {
			LOG.error("Error while serializing " + e.getMessage());
			return null;
		}

		KvStateRequest fakeRequest = new KvStateRequest(
			jobID, stateDescriptor.getName(), key.hashCode(), serializedKeyAndNamespace);
		CompletableFuture<ValueState<T>> future = getState(fakeRequest, false).thenApply(
			stateResponse -> {
				try {
					return stateDescriptor.bind(new ImmutableStateBinder(stateResponse.getContent()));
				} catch (Exception e) {
					throw new FlinkRuntimeException(e);
				}
			});

		// synchronous call
		ValueState<T> valueState = future
			.handle((state, exception) -> {
				if (exception != null) {
					LOG.error("Error in querying state " + exception.getMessage());
					return null;
				}
				return state;
			}).get();

		return valueState != null ? valueState.value() : null;
	}

	@Override
	public <K, T> void set(ValueStateDescriptor<T> stateDescriptor, TypeInformation<K> keyType, K key, T value) throws Exception {
		throw new NotImplementedException();
	}

	@Override
	public <K, T> boolean testAndSet(ValueStateDescriptor<T> stateDescriptor, TypeInformation<K> keyType, K key, T value, Predicate<T> test) throws Exception {
		throw new NotImplementedException();
	}
}
