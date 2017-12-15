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

    public static final String HASH_1 = "MD5";
    public static final String HASH_2 = "SHA-256";

    private final int blockSize;
    private final File file;
    private final HashStorage compressedHashes;
    private final HashStorage uncompressedHashes;
    private final Counters counters;
    private boolean detectCollisions;

    public ProcessTask(int blockSize, File file, HashStorage compressedHashes, HashStorage uncompressedHashes, Counters counters, boolean detectCollisions) {
        this.blockSize = blockSize;
        this.file = file;
        this.compressedHashes = compressedHashes;
        this.uncompressedHashes = uncompressedHashes;
        this.counters = counters;
        this.detectCollisions = detectCollisions;
    }

    @Override
    public void run() {
        try (
            BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file))
        ) {
            byte[] block = new byte[blockSize];

            MessageDigest uncompressedMD1 = MessageDigest.getInstance(HASH_1);
            MessageDigest uncompressedMD2 = MessageDigest.getInstance(HASH_2);

            MessageDigest compressedMD1 = MessageDigest.getInstance(HASH_1);
            MessageDigest compressedMD2 = MessageDigest.getInstance(HASH_2);

            int read;
            while ((read = reader.read(block)) != -1) {

                counters.inputData.addAndGet(read);

                byte[] compressedBlock = compressBlock(block, read);
                counters.compressedData.addAndGet(compressedBlock.length);

                if (consume(block, read, uncompressedMD1, uncompressedMD2, uncompressedHashes)) {
                    counters.dedupData.addAndGet(read);
                    counters.dedupCompressData.addAndGet(compressedBlock.length);
                }

                if (consume(compressedBlock, compressedBlock.length, compressedMD1, compressedMD2, compressedHashes)) {
                    counters.compressedDedupData.addAndGet(compressedBlock.length);
                }

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

    private boolean consume(byte[] block, int count, MessageDigest mdHash1, MessageDigest mdHash2, HashStorage storage) {
        mdHash1.reset();
        mdHash2.reset();

        mdHash1.update(block, 0, count);
        mdHash2.update(block, 0, count);

        byte[] checksum1 = mdHash1.digest();
        byte[] checksum2 = mdHash2.digest();

        boolean unique1;
        boolean unique2;

        /**
         * Alas, need to preserve atomicity on this paired operation.
         * Otherwise, we can get "false" collision when several threads try to add
         * the same pair of checksums.
         */
        if (detectCollisions) {
            synchronized (storage) {
                unique1 = storage.add(checksum1);
                unique2 = storage.add(checksum2);
            }

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
        } else {
            unique1 = storage.add(checksum1);
            unique2 = storage.add(checksum2);
        }

        return unique1 && unique2;
    }


}
