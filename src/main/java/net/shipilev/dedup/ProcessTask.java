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

    private static final ThreadLocal<MessageDigest> MDS = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance(Main.HASH);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    });
    private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
    private static final ThreadLocalByteArray UNCOMP_BUFS = new ThreadLocalByteArray(Main.BLOCK_SIZE);
    private static final int MAX_COMP_LEN = FACTORY.fastCompressor().maxCompressedLength(Main.BLOCK_SIZE);
    private static final ThreadLocalByteArray COMP_BUFS = new ThreadLocalByteArray(MAX_COMP_LEN);

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
            byte[] uncompBlock = UNCOMP_BUFS.get();
            byte[] compBlock = COMP_BUFS.get();

            MessageDigest md = MDS.get();

            int read;
            int lastRead = Main.BLOCK_SIZE;
            while ((read = reader.read(uncompBlock)) != -1) {
                if (lastRead != Main.BLOCK_SIZE) {
                    throw new IllegalStateException("Truncated read detected");
                }
                counters.inputData.addAndGet(read);

                LZ4Compressor lz4 = FACTORY.fastCompressor();
                int compLen = lz4.compress(uncompBlock, 0, read, compBlock, 0, MAX_COMP_LEN);

                counters.compressedData.addAndGet(compLen);

                md.update(uncompBlock, 0, read);
                if (hashes.add(md.digest())) {
                    counters.dedupData.addAndGet(read);
                    counters.dedupCompressData.addAndGet(compLen);
                }

                lastRead = read;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
