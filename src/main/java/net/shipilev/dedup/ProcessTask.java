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

import net.shipilev.dedup.storage.HashStorage;
import net.shipilev.dedup.streams.GZIPOutputStreamEx;

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

            int read;
            int lastRead = blockSize;
            while ((read = reader.read(block)) != -1) {
                if (lastRead != blockSize) {
                    throw new IllegalStateException("Truncated read detected");
                }
                counters.inputData.addAndGet(read);

                byte[] compressedBlock = compressBlock(block, read);
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

    private byte[] compressBlock(byte[] block, int size) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        GZIPOutputStreamEx blockCompress = new GZIPOutputStreamEx(baos, size);
        blockCompress.write(block);
        blockCompress.finish();
        blockCompress.flush();
        blockCompress.close();
        return baos.toByteArray();
    }

    private boolean consume(byte[] block, int count, MessageDigest mdHash, HashStorage storage) {
        mdHash.reset();
        mdHash.update(block, 0, count);
        byte[] checksum1 = mdHash.digest();
        return storage.add(checksum1);
    }

}
