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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.RecursiveAction;

public class ProcessTask extends RecursiveAction {

    private static final int TARGET_SMALL_SIZE = 1 << 17; //  128K per read
    private static final int TARGET_LARGE_SIZE = 1 << 20; // 1024K per read

    private static final ThreadLocalByteArray SMALL_READ_BUFS = new ThreadLocalByteArray(selectBufferSize(TARGET_SMALL_SIZE));
    private static final ThreadLocalByteArray LARGE_READ_BUFS = new ThreadLocalByteArray(selectBufferSize(TARGET_LARGE_SIZE));

    private static int selectBufferSize(int target) {
        int size = 1;
        for (int mult = 0; (mult < 20) && (size < target); mult++) {
            size = Main.BLOCK_SIZE * 1024 * (1 << mult);
        }
        return size;
    }

    private static byte[] selectBuffer(long size) {
        if (size >= TARGET_LARGE_SIZE) {
            return LARGE_READ_BUFS.get();
        } else {
            return SMALL_READ_BUFS.get();
        }
    }

    private final Path path;
    private final HashStorage hashes;
    private final Counters counters;

    public ProcessTask(Path path, HashStorage hashes, Counters counters) {
        this.path = path;
        this.hashes = hashes;
        this.counters = counters;
    }

    @Override
    protected void compute() {
        int blockSize = Main.BLOCK_SIZE * 1024;

        File file = path.toFile();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] readBuf = selectBuffer(file.length());

            int read;
            while ((read = fis.read(readBuf)) != -1) {
                int bufCount = (read % blockSize == 0) ?
                        (read / blockSize) :
                        (read / blockSize) + 1;

                int[] sizes = new int[bufCount];
                CompressTask[] cts = new CompressTask[bufCount];
                HashTask[] hts = new HashTask[bufCount];

                for (int b = 0; b < bufCount; b++) {
                    int start = b * blockSize;
                    int size = Math.min(read - start, blockSize);

                    cts[b] = new CompressTask(readBuf, start, size);
                    cts[b].fork();

                    hts[b] = new HashTask(readBuf, start, size);
                    hts[b].fork();

                    sizes[b] = size;
                }

                for (int b = 0; b < bufCount; b++) {
                    int size = sizes[b];
                    counters.inputData.addAndGet(size);

                    cts[b].join();
                    int compLen = cts[b].compSize();

                    counters.compressedData.addAndGet(compLen);

                    hts[b].join();
                    byte[] hash = hts[b].digest();

                    if (hash != null && hashes.add(hash)) {
                        counters.dedupData.addAndGet(size);
                        counters.dedupCompressData.addAndGet(compLen);
                    }
                }
            }

            counters.processedFiles.incrementAndGet();
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
