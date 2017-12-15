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

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ProcessTask implements Runnable {

    public static final String HASH = "SHA-256";

    private final int blockSize;
    private final File file;
    private final HashStorage compressedHashes;
    private final HashStorage uncompressedHashes;
    private final Counters counters;

    public ProcessTask(int blockSize, File file, HashStorage compressedHashes, HashStorage uncompressedHashes, Counters counters) {
        this.blockSize = blockSize;
        this.file = file;
        this.compressedHashes = compressedHashes;
        this.uncompressedHashes = uncompressedHashes;
        this.counters = counters;
    }

    @Override
    public void run() {
        try (
            BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file))
        ) {
            byte[] block = new byte[blockSize];

            MessageDigest hashUncomp = MessageDigest.getInstance(HASH);
            MessageDigest hashComp = MessageDigest.getInstance(HASH);

            LZ4Factory factory = LZ4Factory.fastestInstance();

            int read;
            int lastRead = blockSize;
            while ((read = reader.read(block)) != -1) {
                if (lastRead != blockSize) {
                    throw new IllegalStateException("Truncated read detected");
                }
                counters.inputData.addAndGet(read);

                byte[] compressedBlock = compressBlock(factory, block, read);
                counters.compressedData.addAndGet(compressedBlock.length);

                if (consume(block, read, hashUncomp, uncompressedHashes)) {
                    counters.dedupData.addAndGet(read);
                    counters.dedupCompressData.addAndGet(compressedBlock.length);
                }

                if (consume(compressedBlock, compressedBlock.length, hashComp, compressedHashes)) {
                    counters.compressedDedupData.addAndGet(compressedBlock.length);
                }

                lastRead = read;
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private byte[] compressBlock(LZ4Factory factory, byte[] block, int size) throws IOException {
        LZ4Compressor lz4 = factory.fastCompressor();
        int maxLen = lz4.maxCompressedLength(size);
        byte[] compBlock = new byte[maxLen];
        int compLen = lz4.compress(block, 0, size, compBlock, 0, maxLen);
        return Arrays.copyOf(compBlock, compLen);
    }

    private boolean consume(byte[] block, int count, MessageDigest mdHash, HashStorage storage) {
        mdHash.reset();
        mdHash.update(block, 0, count);
        byte[] checksum1 = mdHash.digest();
        return storage.add(checksum1);
    }

}
