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

import net.shipilev.dedup.storage.BerkeleyHashStorage;
import net.shipilev.dedup.storage.DerbyHashStorage;
import net.shipilev.dedup.storage.H2HashStorage;
import net.shipilev.dedup.storage.HashStorage;
import net.shipilev.dedup.storage.InMemoryHashStorage;

import java.io.File;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final int QUEUE_SIZE = 10 * 1000 * 1000;
    private static final long POLL_INTERVAL_SEC = 1;

    private static String path;
    private static String storage;
    private static Integer blockSize;

    private HashStorage uncompressedHashes;
    private HashStorage compressedHashes;

    private BlockingQueue<File> processQueue = new ArrayBlockingQueue<File>(QUEUE_SIZE);

    private Counters uncompressedCounters = new Counters();
    private Counters compressedCounters = new Counters();

    private int printCounter;
    private long lastSize;

    public static void main(String[] args) throws NoSuchAlgorithmException, DigestException, InterruptedException {

        path = ".";
        if (args.length > 0) {
            path = args[0];
        }

        storage = "inmemory";
        if (args.length > 1) {
            storage = args[1];
        }

        blockSize = 4096;
        if (args.length > 2) {
            blockSize = Integer.valueOf(args[2]);
        }

        new Main().run();
    }

    private void createStorages(String storage) {
        switch (storage) {
            case "inmemory":
                compressedHashes = new InMemoryHashStorage();
                uncompressedHashes = new InMemoryHashStorage();
                break;
            case "berkeley":
                compressedHashes = new BerkeleyHashStorage("hashes-compressed");
                uncompressedHashes = new BerkeleyHashStorage("hashes-uncompressed");
                break;
            case "h2":
                compressedHashes = new H2HashStorage("hashes-compressed");
                uncompressedHashes = new H2HashStorage("hashes-uncompressed");
                break;
            case "derby":
                compressedHashes = new DerbyHashStorage("hashes-compressed");
                uncompressedHashes = new DerbyHashStorage("hashes-uncompressed");
                break;
            default:
                throw new IllegalStateException("Unknown storage " + storage);
        }
    }

    private void run() throws InterruptedException {
        createStorages(storage);

        System.err.println("Using " + blockSize + "-byte blocks");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new PrintDetails(), 1, POLL_INTERVAL_SEC, TimeUnit.SECONDS);

        Thread enumerator = new Thread(new EnumeratorWorker(new File(path), processQueue));
        enumerator.start();

        List<ReaderWorker> workers = new ArrayList<ReaderWorker>();
        for (int c = 0; c < Runtime.getRuntime().availableProcessors() / 2 + 1; c++) {
            ReaderWorker worker = new ReaderWorker(blockSize, processQueue, compressedHashes, uncompressedHashes, compressedCounters, uncompressedCounters);
            workers.add(worker);
            worker.start();
        }

        enumerator.join();

        for (ReaderWorker worker : workers) {
            worker.finish();
        }

        executor.shutdownNow();

        printProgress(true);
    }


    private void printProgress(boolean proceedAlways) {
        printCounter++;
        if (!proceedAlways && (printCounter & (16384 - 1)) == 0) {
            return;
        }

        System.err.printf("Running at %5.2f Kbps (%5.2f Gb/hour), %d/%d files in queue\n",
                (uncompressedCounters.inputData.get() - lastSize) * 1.0 / 1024 / POLL_INTERVAL_SEC,
                (uncompressedCounters.inputData.get() - lastSize) * 3600.0 / 1024 / 1024 / 1024 / POLL_INTERVAL_SEC,
                processQueue.size(),
                QUEUE_SIZE);

        lastSize = uncompressedCounters.inputData.get();

        System.err.printf("COMPRESS:       %5.2fx increase. %d Kb --(compress)--> %d Kb\n",
                uncompressedCounters.inputData.get() * 1.0 / compressedCounters.inputData.get(),
                uncompressedCounters.inputData.get() / 1024,
                compressedCounters.inputData.get() / 1024
        );

        System.err.printf("COMPRESS+DEDUP: %5.2fx increase. %d Kb --(compress)--> %d Kb ------(dedup)-------> %d Kb\n",
                uncompressedCounters.inputData.get() * 1.0 / (compressedCounters.inputData.get() - compressedCounters.duplicatedData.get()),
                uncompressedCounters.inputData.get() / 1024,
                compressedCounters.inputData.get() / 1024,
                (compressedCounters.inputData.get() - compressedCounters.duplicatedData.get()) / 1024
        );

        System.err.printf("DEDUP:          %5.2fx increase, %d Kb ----(dedup)---> %d Kb\n",
                uncompressedCounters.inputData.get() * 1.0 / (uncompressedCounters.inputData.get() - uncompressedCounters.duplicatedData.get()),
                uncompressedCounters.inputData.get() / 1024,
                (uncompressedCounters.inputData.get() - uncompressedCounters.duplicatedData.get()) / 1024
        );

        System.err.printf("DEDUP+COMPRESS: %5.2fx increase. %d Kb ----(dedup)---> %d Kb --(block-compress)--> %d Kb\n",
                uncompressedCounters.inputData.get() * 1.0 / uncompressedCounters.compressedBlockSize.get(),
                uncompressedCounters.inputData.get() / 1024,
                (uncompressedCounters.inputData.get() - uncompressedCounters.duplicatedData.get()) / 1024,
                uncompressedCounters.compressedBlockSize.get() / 1024
        );

        System.err.printf("detected collisions: %d on %s, %d on %s\n",
                uncompressedCounters.collisions1.get() + compressedCounters.collisions1.get(),
                ProcessorWorker.HASH_1,
                uncompressedCounters.collisions2.get() + compressedCounters.collisions2.get(),
                ProcessorWorker.HASH_2
        );

        System.err.println();
    }


    private class PrintDetails implements Runnable {
        @Override
        public void run() {
            printProgress(true);
        }
    }
}
