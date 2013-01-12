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
package net.shipilev.dedup;

import net.shipilev.dedup.streams.CountingOutputStream;
import net.shipilev.dedup.streams.GZIPOutputStreamEx;
import net.shipilev.dedup.storage.HashStorage;
import net.shipilev.dedup.streams.NullOutputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ProcessorWorker implements Runnable {
    private final int blockSize;
    private final InputStream stream;
    private final HashStorage hashStorage;
    private final Counters counters;
    private boolean doingBlockCompress;
    public static final String HASH_1 = "MD5";
    public static final String HASH_2 = "SHA-256";

    public ProcessorWorker(int blockSize, InputStream stream, HashStorage hashStorage, Counters counters, boolean doingBlockCompress) {
        this.blockSize = blockSize;
        this.stream = stream;
        this.hashStorage = hashStorage;
        this.counters = counters;
        this.doingBlockCompress = doingBlockCompress;
    }

    @Override
    public void run() {
        try {
            byte[] block = new byte[blockSize];

            MessageDigest mdHash1 = MessageDigest.getInstance(HASH_1);
            MessageDigest mdHash2 = MessageDigest.getInstance(HASH_2);

            BufferedInputStream reader = new BufferedInputStream(stream);
            int read;
            while ((read = reader.read(block)) != -1) {
                mdHash1.reset();
                mdHash2.reset();

                mdHash1.update(block, 0, read);
                mdHash2.update(block, 0, read);

                byte[] checksum1 = mdHash1.digest();
                byte[] checksum2 = mdHash2.digest();

                boolean unique1;
                boolean unique2;

                /**
                 * Alas, need to preserve atomicity on this paired operation.
                 * Otherwise, we can get "false" collision when several threads try to add
                 * the same pair of checksums.
                 */
                synchronized (hashStorage) {
                    unique1 = hashStorage.add(checksum1);
                    unique2 = hashStorage.add(checksum2);
                }

                if (!unique1 || !unique2) {
                    counters.duplicatedData.addAndGet(read);
                } else {
                    if (doingBlockCompress) {
                        CountingOutputStream blockCompressCounter = new CountingOutputStream(new NullOutputStream());
                        GZIPOutputStreamEx blockCompress = new GZIPOutputStreamEx(blockCompressCounter);
                        blockCompress.write(block);
                        blockCompress.finish();
                        blockCompress.flush();
                        blockCompress.close();

                        counters.compressedBlockSize.addAndGet(blockCompressCounter.getCount());
                    }
                }

                counters.inputData.addAndGet(read);

                if (unique1 && !unique2) {
                    System.err.println("Whoa! Collision on " + HASH_1 + "\n"
                            + "block = " + Arrays.toString(block));
                    counters.collisions1.incrementAndGet();
                }
                if (!unique1 && unique2) {
                    System.err.println("Whoa! Collision on " + HASH_2 + "\n"
                            + "block = " + Arrays.toString(block));
                    counters.collisions2.incrementAndGet();
                }
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }


}
