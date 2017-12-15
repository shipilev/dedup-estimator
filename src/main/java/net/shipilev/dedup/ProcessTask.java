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
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ProcessTask implements Runnable {

    public static final String HASH = "SHA-256";

    private final int blockSize;
    private final File file;
    private final HashStorage compressedHashes;
    private final HashStorage uncompressedHashes;
    private final Counters counters;
    private final LZ4Factory factory;
    private final ThreadLocal<MessageDigest> mds;
    private final ThreadLocalByteArray uncompBufs;
    private final ThreadLocalByteArray compBufs;
    private final int maxCompLen;

    public ProcessTask(int blockSize, File file, HashStorage compressedHashes, HashStorage uncompressedHashes, Counters counters) {
        this.blockSize = blockSize;
        this.file = file;
        this.compressedHashes = compressedHashes;
        this.uncompressedHashes = uncompressedHashes;
        this.counters = counters;
        this.factory = LZ4Factory.fastestInstance();
        this.mds = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance(HASH);
            } catch (NoSuchAlgorithmException e) {
                return null;
            }
        });
        this.uncompBufs = new ThreadLocalByteArray(blockSize);
        this.maxCompLen = factory.fastCompressor().maxCompressedLength(blockSize);
        this.compBufs = new ThreadLocalByteArray(maxCompLen);
    }

    @Override
    public void run() {
        try (
            BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file))
        ) {
            byte[] uncompBlock = uncompBufs.get();
            byte[] compBlock = compBufs.get();

            MessageDigest md = mds.get();

            int read;
            int lastRead = blockSize;
            while ((read = reader.read(uncompBlock)) != -1) {
                if (lastRead != blockSize) {
                    throw new IllegalStateException("Truncated read detected");
                }
                counters.inputData.addAndGet(read);

                LZ4Compressor lz4 = factory.fastCompressor();
                int compLen = lz4.compress(uncompBlock, 0, read, compBlock, 0, maxCompLen);

                counters.compressedData.addAndGet(compLen);

                md.update(uncompBlock, 0, read);
                if (uncompressedHashes.add(md.digest())) {
                    counters.dedupData.addAndGet(read);
                    counters.dedupCompressData.addAndGet(compLen);
                }

                md.reset();
                md.update(compBlock, 0, compLen);
                if (compressedHashes.add(md.digest())) {
                    counters.compressedDedupData.addAndGet(compLen);
                }

                lastRead = read;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
