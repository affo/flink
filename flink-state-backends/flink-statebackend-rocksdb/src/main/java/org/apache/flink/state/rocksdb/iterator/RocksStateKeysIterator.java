/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.rocksdb.RocksIteratorWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over
 * the keys. This class is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 */
public class RocksStateKeysIterator<K> extends AbstractRocksStateKeysIterator<K>
        implements Iterator<K> {

    @Nonnull private final byte[] namespaceBytes;

    private K nextKey;
    private K previousKey;

    public RocksStateKeysIterator(
            @Nonnull RocksIteratorWrapper iterator,
            @Nonnull String state,
            @Nonnull TypeSerializer<K> keySerializer,
            int keyGroupPrefixBytes,
            boolean ambiguousKeyPossible,
            @Nonnull byte[] namespaceBytes) {
        super(iterator, state, keySerializer, keyGroupPrefixBytes, ambiguousKeyPossible);
        this.namespaceBytes = namespaceBytes;
        this.nextKey = null;
        this.previousKey = null;
    }

    @Override
    public boolean hasNext() {
        try {
            while (nextKey == null && iterator.isValid()) {

                final byte[] keyBytes = iterator.key();
                final K currentKey = deserializeKey(keyBytes, byteArrayDataInputView);

                if (isMatchingNameSpace(
                                keyBytes, byteArrayDataInputView.getPosition(), namespaceBytes)
                        && !Objects.equals(previousKey, currentKey)) {
                    previousKey = currentKey;
                    nextKey = currentKey;
                }
                iterator.next();
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
        }
        return nextKey != null;
    }

    @Override
    public K next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Failed to access state [" + state + "]");
        }

        K tmpKey = nextKey;
        nextKey = null;
        return tmpKey;
    }
}
