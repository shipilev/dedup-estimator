/*
 * Copyright 2010 Aleksey Shipilev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.shipilev.dedup.storage;

import net.shipilev.dedup.streams.ByteArrayWrapper;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryHashStorage implements HashStorage {

    private final Set<ByteArrayWrapper> storage = Collections.newSetFromMap(new ConcurrentHashMap<ByteArrayWrapper, Boolean>());

    public InMemoryHashStorage() {
        System.err.println("Using InMemory datastorage, ConcurrentHashMap-based");
    }

    @Override
    public boolean add(byte[] data) {
        return storage.add(new ByteArrayWrapper(data));
    }
}
