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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessTask extends RecursiveAction {

    private static final ThreadLocalByteArray READ_BUFS;

    static {
        final int TARGET_SIZE = 1 << 22; // 4 M per read
        int size = 1;
        for (int mult = 0; (mult < 20) && (size < TARGET_SIZE); mult++) {
            size = Main.BLOCK_SIZE * 1024 * (1 << mult);
        }
        READ_BUFS = new ThreadLocalByteArray(size);
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
    protected void compute() {
        int blockSize = Main.BLOCK_SIZE * 1024;

        try (RandomAccessFile raf = new RandomAccessFile(file.toString(), "r")) {
            AtomicLong inputData = counters.inputData;
            AtomicLong compressedData = counters.compressedData;
            AtomicLong dedupData = counters.dedupData;
            AtomicLong dedupCompressData = counters.dedupCompressData;

            counters.processedFiles.incrementAndGet();

            byte[] readBuf = READ_BUFS.get();

            int read;
            while ((read = raf.read(readBuf)) != -1) {
                int bufCount = (read % blockSize == 0) ?
                        (read / blockSize) :
                        (read / blockSize) + 1;

                int[] sizes = new int[bufCount];
                CompressTask[] cts = new CompressTask[bufCount];
                HashTask[] hts = new HashTask[bufCount];
                RecursiveAction[] all = new RecursiveAction[bufCount*2];

                for (int b = 0; b < bufCount; b++) {
                    int start = b * blockSize;
                    int size = Math.min(read - start, blockSize);
                    sizes[b] = size;

                    CompressTask ct = new CompressTask(readBuf, start, size);
                    HashTask ht = new HashTask(readBuf, start, size);

                    cts[b] = ct;
                    hts[b] = ht;
                    all[b] = ct;
                    all[bufCount + b] = ht;
                }

                ForkJoinTask.invokeAll(all);

                for (int b = 0; b < bufCount; b++) {
                    int compLen = cts[b].compSize();
                    byte[] hash = hts[b].digest();
                    int size = sizes[b];

                    inputData.addAndGet(size);
                    compressedData.addAndGet(compLen);

                    if (hash != null && hashes.add(hash)) {
                        dedupData.addAndGet(size);
                        dedupCompressData.addAndGet(compLen);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class CompressTask extends RecursiveAction {
        private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
        private static final int MAX_COMP_LEN = FACTORY.fastCompressor().maxCompressedLength(Main.BLOCK_SIZE * 1024);
        private static final ThreadLocalByteArray COMP_BUFS = new ThreadLocalByteArray(MAX_COMP_LEN);

        private final byte[] buf;
        private final int start;
        private final int size;
        int compSize;

        public CompressTask(byte[] buf, int start, int size) {
            this.buf = buf;
            this.start = start;
            this.size = size;
        }

        @Override
        protected void compute() {
            if (Main.DO_COMPRESS) {
                LZ4Compressor lz4 = FACTORY.fastCompressor();
                byte[] compBlock = COMP_BUFS.get();
                compSize = lz4.compress(buf, start, size, compBlock, 0, MAX_COMP_LEN);
            } else {
                compSize = size;
            }
        }

        public int compSize() {
            return compSize;
        }
    }

    static class HashTask extends RecursiveAction {
        private static final ThreadLocal<MessageDigest> MDS =
                ThreadLocal.withInitial(() -> {
                    try {
                        return MessageDigest.getInstance(Main.HASH);
                    } catch (NoSuchAlgorithmException e) {
                        return null;
                    }
                });

        private final byte[] buf;
        private final int start;
        private final int size;
        byte[] digest;

        public HashTask(byte[] buf, int start, int size) {
            this.buf = buf;
            this.start = start;
            this.size = size;
        }

        @Override
        protected void compute() {
            if (Main.DO_DEDUP) {
                MessageDigest md = MDS.get();
                md.reset();
                md.update(buf, start, size);
                digest = md.digest();
            }
        }

        public byte[] digest() {
            return digest;
        }
    }

}
