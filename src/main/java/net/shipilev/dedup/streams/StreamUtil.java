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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamUtil {
    private static final ThreadLocalByteArray BUFFERS = new ThreadLocalByteArray();

    public static void copy(InputStream inputStream, OutputStream... outputStreams) throws IOException {
        byte[] buffer = BUFFERS.get();

        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            for (OutputStream out : outputStreams) {
                out.write(buffer, 0, read);
            }
        }
    }
}
