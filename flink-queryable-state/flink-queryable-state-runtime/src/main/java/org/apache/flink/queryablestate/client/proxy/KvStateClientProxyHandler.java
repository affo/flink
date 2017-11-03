/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.queryablestate.client.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.queryablestate.server.GlobalStateGateway;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.util.Preconditions;

import java.net.ConnectException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * This handler acts as an internal (to the Flink cluster) client that receives
 * the requests from external clients, executes them by contacting the Job Manager (if necessary) and
 * the Task Manager holding the requested state, and forwards the answer back to the client.
 */
@Internal
@ChannelHandler.Sharable
public class KvStateClientProxyHandler extends AbstractServerHandler<KvStateRequest, KvStateResponse> {
	private final GlobalStateGateway globalStateGateway;

	/**
	 * Create the handler used by the {@link KvStateClientProxyImpl}.
	 *
	 * @param proxy                the {@link KvStateClientProxyImpl proxy} using the handler.
	 * @param queryExecutorThreads the number of threads used to process incoming requests.
	 * @param serializer           the {@link MessageSerializer} used to (de-) serialize the different messages.
	 * @param stats                server statistics collector.
	 */
	public KvStateClientProxyHandler(
		final KvStateClientProxyImpl proxy,
		final int queryExecutorThreads,
		final MessageSerializer<KvStateRequest, KvStateResponse> serializer,
		final KvStateRequestStats stats) {

		super(proxy, serializer, stats);
		Preconditions.checkNotNull(proxy);

		this.globalStateGateway = new GlobalStateGateway(proxy, queryExecutor, getServerName(), queryExecutorThreads);
	}

	@Override
	public CompletableFuture<KvStateResponse> handleRequest(
		final long requestId,
		final KvStateRequest request) {
		CompletableFuture<KvStateResponse> response = new CompletableFuture<>();
		executeActionAsync(response, request, false);
		return response;
	}

	private void executeActionAsync(
		final CompletableFuture<KvStateResponse> result,
		final KvStateRequest request,
		final boolean update) {

		if (!result.isDone()) {
			final CompletableFuture<KvStateResponse> operationFuture = globalStateGateway.getState(request, update);
			operationFuture.whenCompleteAsync(
				(t, throwable) -> {
					if (throwable != null) {
						if (throwable instanceof CancellationException) {
							result.completeExceptionally(throwable);
						} else if (throwable.getCause() instanceof UnknownKvStateIdException ||
							throwable.getCause() instanceof UnknownKvStateKeyGroupLocationException ||
							throwable.getCause() instanceof UnknownKvStateLocation ||
							throwable.getCause() instanceof ConnectException) {

							// These failures are likely to be caused by out-of-sync
							// KvStateLocation. Therefore we retry this query and
							// force look up the location.

							executeActionAsync(result, request, true);
						} else {
							result.completeExceptionally(throwable);
						}
					} else {
						result.complete(t);
					}
				}, queryExecutor);

			result.whenComplete(
				(t, throwable) -> operationFuture.cancel(false));
		}
	}

	@Override
	public void shutdown() {
		globalStateGateway.shutdown();
	}
}
