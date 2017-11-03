package org.apache.flink.statesharing.examples;

import org.apache.flink.api.common.functions.GlobalStateClient;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 31/10/17.
 */
public class TestDrive {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		LocalQueryableStateEnvironment env = LocalQueryableStateEnvironment.createLocalEnvironment(config);

		ValueStateDescriptor<Long> counters = new ValueStateDescriptor<>("counters", Long.class);

		DataStream<String> words = env
			.fromElements("foo", "bar", "buz", "foo", "foo", "buz")
			// insert pauses
			.map(word -> {
				Thread.sleep(1000);
				return word;
			});

		words.addSink(new Querier(counters)).name("Querier");

		DataStream<Tuple2<String, Long>> counts = words.keyBy(w -> w).map(new WordCounter(counters));

		counts.print();

		env.execute();
	}

	private static class WordCounter extends RichMapFunction<String, Tuple2<String, Long>> {
		private transient ValueState<Long> counter;
		public final ValueStateDescriptor<Long> countersStateDescriptor;

		public WordCounter(ValueStateDescriptor<Long> countersStateDescriptor) {
			this.countersStateDescriptor = countersStateDescriptor;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ValueStateDescriptor<Long> sd = countersStateDescriptor;
			// set queryable with the same name as the state name
			sd.setQueryable(sd.getName());
			counter = getRuntimeContext().getState(sd);
		}

		@Override
		public Tuple2<String, Long> map(String value) throws Exception {
			Long count = counter.value();
			if (count == null) {
				count = 0L;
			}

			count++;
			counter.update(count);
			return Tuple2.of(value, count);
		}
	}

	private static class Querier extends RichSinkFunction<String> {
		private ValueStateDescriptor<Long> stateToQueryDescriptor;
		private transient GlobalStateClient globalState;

		public Querier(ValueStateDescriptor<Long> stateToQuery) {
			super();
			this.stateToQueryDescriptor = stateToQuery;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			globalState = getRuntimeContext().getGlobalStateClient();
		}

		@Override
		public void invoke(String key, Context context) throws Exception {
			Long value = globalState.get(
				stateToQueryDescriptor, // the descriptor of the state to Query
				TypeInformation.of(String.class), // The information type about the key
				key); // the key to be queried
			System.out.println(">>> " + key + ": " + value);
		}
	}
}
