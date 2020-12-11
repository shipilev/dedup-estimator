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

import net.shipilev.dedup.storage.*;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    static final String STORAGE = System.getProperty("storage", "inmemory");
    static final String HASH = System.getProperty("hash", "SHA-256");
    static final int BLOCK_SIZE = Integer.getInteger("blockSize", 128);
    static final int THREADS = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    static final long POLL_INTERVAL_SEC = Integer.getInteger("pollInterval", 1);

    private final Counters counters = new Counters();

    private HashStorage hashes;

    private long firstPoll;

    public static void main(String[] args) {
        String path = ".";
        if (args.length > 0) {
            path = args[0];
        }

        new Main().run(path);
    }

    private void createStorages() {
        switch (STORAGE) {
            case "inmemory":
                hashes = new InMemoryHashStorage();
                break;
            case "berkeley":
                hashes = new BerkeleyHashStorage("hashes");
                break;
            case "h2":
                hashes = new H2HashStorage("hashes");
                break;
            case "derby":
                hashes = new DerbyHashStorage("hashes");
                break;
            default:
                throw new IllegalStateException("Unknown storage " + Main.STORAGE);
        }
    }

    private void run(String path) {
        createStorages();

        System.err.println("Running with " + THREADS + " threads");
        System.err.println("Using " + BLOCK_SIZE + " KB blocks");

        ScheduledExecutorService poller = Executors.newScheduledThreadPool(1);
        poller.scheduleAtFixedRate(this::printProgress, POLL_INTERVAL_SEC, POLL_INTERVAL_SEC, TimeUnit.SECONDS);
        firstPoll = System.nanoTime();

        ForkJoinPool fjp = new ForkJoinPool(THREADS);
        fjp.invoke(new WalkTask(new File(path).toPath(), hashes, counters));

        poller.shutdownNow();

        System.err.println("FINAL RESULT:");
        System.err.println(path + ", using " + BLOCK_SIZE + " KB blocks");
        printProgress();
    }

    private void printProgress() {
        long queuedDirs = counters.queuedDirs.get();
        long processedDirs = counters.processedDirs.get();
        long queuedFiles = counters.queuedFiles.get();
        long processedFiles = counters.processedFiles.get();
        long queuedData = counters.queuedData.get();
        long inputData = counters.inputData.get();
        long compressedData = counters.compressedData.get();
        long dedupData = counters.dedupData.get();
        long dedupCompressData = counters.dedupCompressData.get();

        // Avoid division by zero:
        if (inputData == 0)         inputData = 1;
        if (compressedData == 0)    compressedData = 1;
        if (dedupData == 0)         dedupData = 1;
        if (dedupCompressData == 0) dedupCompressData = 1;

        final int K = 1024;
        final int M = K * 1024;
        final int G = M * 1024;
        final long T = G * 1024L;

        System.err.printf("Running at %5.2f MB/sec (%5.2f TB/hour), %d/%d dirs, %d/%d files, %d/%d MB\n",
                (inputData * 1.0 / M * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll + 1),
                (inputData * 3600.0 / T * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll + 1),
                processedDirs,
                queuedDirs,
                processedFiles,
                queuedFiles,
                inputData / M,
                queuedData / M
        );

        System.err.printf("COMPRESS:       %5.3fx increase, %,d MB --(block-compress)--> %,d MB\n",
                inputData * 1.0 / compressedData,
                inputData / M,
                compressedData / M
        );

        System.err.printf("DEDUP:          %5.3fx increase, %,d MB ------(dedup)-------> %,d MB\n",
                inputData * 1.0 / dedupData,
                inputData / M,
                dedupData / M
        );

        System.err.printf("DEDUP+COMPRESS: %5.3fx increase, %,d MB ------(dedup)-------> %,d MB --(block-compress)--> %,d MB\n",
                inputData * 1.0 / dedupCompressData,
                inputData / M,
                dedupData / M,
                dedupCompressData / M
        );

        System.err.println();
    }


}
