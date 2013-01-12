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
package net.shipilev.dedup.streams;

import java.util.Arrays;

public class ByteArrayWrapper {
    private final byte[] array;

    public ByteArrayWrapper(byte[] array) {
        this.array = array;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArrayWrapper)) {
            return false;
        }

        ByteArrayWrapper that = (ByteArrayWrapper) o;

        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return array != null ? Arrays.hashCode(array) : 0;
    }

    public byte[] getArray() {
        return array;
    }
}
