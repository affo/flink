package org.apache.flink.api.common.functions;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.function.Predicate;

/**
 * Created by affo on 03/11/17.
 */
public interface GlobalStateClient {
	<K, T> T get(ValueStateDescriptor<T> stateDescriptor, TypeInformation<K> keyType, K key) throws Exception;

	<K, T> void set(ValueStateDescriptor<T> stateDescriptor, TypeInformation<K> keyType, K key, T value) throws Exception;

	<K, T> boolean testAndSet(
		ValueStateDescriptor<T> stateDescriptor,
		TypeInformation<K> keyType,
		K key,
		T value,
		Predicate<T> test) throws Exception;
}
