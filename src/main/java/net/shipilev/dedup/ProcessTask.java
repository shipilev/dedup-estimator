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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.shipilev.dedup.storage.HashStorage;
import net.shipilev.dedup.streams.ThreadLocalByteArray;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ProcessTask implements Runnable {

    private static final ThreadLocal<MessageDigest> MDS;
    private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
    private static final int MAX_COMP_LEN = FACTORY.fastCompressor().maxCompressedLength(Main.BLOCK_SIZE);
    private static final ThreadLocalByteArray COMP_BUFS = new ThreadLocalByteArray(MAX_COMP_LEN);
    private static final ThreadLocalByteArray READ_BUFS;

    static {
        final int TARGET_SIZE = 1 << 20; // 1 M per thread
        int size = 1;
        for (int mult = 0; (mult < 20) && (size < TARGET_SIZE); mult++) {
            size = Main.BLOCK_SIZE * (1 << mult);
        }
        READ_BUFS = new ThreadLocalByteArray(size);

        MDS = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance(Main.HASH);
            } catch (NoSuchAlgorithmException e) {
                return null;
            }
        });
    }

    private final Path file;
    private final HashStorage hashes;
    private final Counters counters;

    public ProcessTask(Path file, HashStorage hashes, Counters counters) {
        this.file = file;
        this.hashes = hashes;
        this.counters = counters;
    }

    @Override
    public void run() {
        try (
            InputStream reader = Files.newInputStream(file)
        ) {
            byte[] readBuf = READ_BUFS.get();
            byte[] compBlock = COMP_BUFS.get();

            MessageDigest md = MDS.get();

            int read;
            while ((read = reader.read(readBuf)) != -1) {
                int inputData = 0;
                int compressedData = 0;
                int dedupData = 0;
                int dedupCompressData = 0;

                for (int start = 0; start < read; start += Main.BLOCK_SIZE) {
                    int size = Math.min(read - start, Main.BLOCK_SIZE);

                    inputData += size;

                    LZ4Compressor lz4 = FACTORY.fastCompressor();
                    int compLen = lz4.compress(readBuf, start, size, compBlock, 0, MAX_COMP_LEN);

                    compressedData += compLen;

                    md.update(readBuf, start, size);
                    if (hashes.add(md.digest())) {
                        dedupData += size;
                        dedupCompressData += size;
                    }
                }

                counters.inputData.addAndGet(inputData);
                counters.compressedData.addAndGet(compressedData);
                counters.dedupData.addAndGet(dedupData);
                counters.dedupCompressData.addAndGet(dedupCompressData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
