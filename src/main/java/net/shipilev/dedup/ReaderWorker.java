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

import net.shipilev.dedup.streams.GZIPOutputStreamEx;
import net.shipilev.dedup.storage.HashStorage;
import net.shipilev.dedup.streams.StreamUtil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPOutputStream;

public class ReaderWorker implements Runnable {
    private final int blockSize;
    private final BlockingQueue<File> filesToProcess;
    private final HashStorage compressedHashes;
    private final HashStorage uncompressedHashes;
    private final Counters compressedCounters;
    private final Counters uncompressedCounters;
    private volatile boolean isWorking;
    private Thread thread;

    public ReaderWorker(int blockSize, BlockingQueue<File> filesToProcess, HashStorage compressedHashes, HashStorage uncompressedHashes,
                        Counters compressedCounters, Counters uncompressedCounters) {
        this.blockSize = blockSize;
        this.filesToProcess = filesToProcess;
        this.compressedHashes = compressedHashes;
        this.uncompressedHashes = uncompressedHashes;
        this.compressedCounters = compressedCounters;
        this.uncompressedCounters = uncompressedCounters;
    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        File file;
        try {
            while ((file = filesToProcess.take()) != null) {
                isWorking = true;
                try {
                    readFile(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                isWorking = false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void readFile(File file) throws IOException {
        FileInputStream fileStream = new FileInputStream(file);

        PipedInputStream compressedStream = new PipedInputStream(blockSize);
        PipedOutputStream pos = new PipedOutputStream(compressedStream);
        GZIPOutputStream sink1 = new GZIPOutputStreamEx(pos);

        PipedInputStream uncompressedStream = new PipedInputStream(blockSize);
        PipedOutputStream sink2 = new PipedOutputStream(uncompressedStream);

        Thread compressedThread = new Thread(new ProcessorWorker(blockSize, compressedStream, compressedHashes, compressedCounters, false));
        compressedThread.start();

        Thread uncompressedThread = new Thread(new ProcessorWorker(blockSize, uncompressedStream, uncompressedHashes, uncompressedCounters, true));
        uncompressedThread.start();

        StreamUtil.copy(new BufferedInputStream(fileStream, 64 * 1024), sink1, sink2);

        sink1.close();
        sink2.close();

        try {
            compressedThread.join();
            uncompressedThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        fileStream.close();
    }

    public void finish() {
        while (isWorking || filesToProcess.size() > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // swallow
        }
    }
}
