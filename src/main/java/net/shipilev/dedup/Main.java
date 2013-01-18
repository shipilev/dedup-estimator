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
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;

public class Main {

    private static final String STORAGE = System.getProperty("storage", "inmemory");
    private static final int QUEUE_SIZE = Integer.getInteger("queueSize", 1000 * 1000);
    private static final int BLOCK_SIZE = Integer.getInteger("blockSize", 4096);
    private static final int THREADS = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    private static final long POLL_INTERVAL_SEC = Integer.getInteger("pollInterval", 1);
    private static final boolean DETECT_COLLISIONS = Boolean.getBoolean("collisions");

    private final BlockingQueue<Runnable> abq = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final Counters counters = new Counters();

    private HashStorage uncompressedHashes;
    private HashStorage compressedHashes;

    private int printCounter;
    private long lastSize;

    public static void main(String[] args) throws NoSuchAlgorithmException, DigestException, InterruptedException, IOException {
        String path = ".";
        if (args.length > 0) {
            path = args[0];
        }

        new Main().run(path);
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

    private void run(String path) throws InterruptedException, IOException {
        createStorages(STORAGE);

        System.err.println("Running with " + THREADS + " threads");
        System.err.println("Using " + BLOCK_SIZE + "-byte blocks");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new PrintDetails(), 1, POLL_INTERVAL_SEC, TimeUnit.SECONDS);


        final ThreadPoolExecutor tpe = new ThreadPoolExecutor(THREADS, THREADS, 1, TimeUnit.DAYS, abq);
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        Files.walkFileTree(new File(path).toPath(), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isSymbolicLink()) {
                    tpe.submit(new ProcessTask(BLOCK_SIZE, file.toFile(), uncompressedHashes, compressedHashes, counters, DETECT_COLLISIONS));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        tpe.shutdown();
        executor.shutdownNow();

        while (!tpe.awaitTermination(1, TimeUnit.SECONDS)) {
            printProgress(true);
        }

        System.err.println("FINAL RESULT:");
        System.err.println(path + ", using " + BLOCK_SIZE + "-byte blocks");
        printProgress(true);
    }


    private void printProgress(boolean proceedAlways) {
        printCounter++;
        if (!proceedAlways && (printCounter & (16384 - 1)) == 0) {
            return;
        }

        long inputData = counters.inputData.get();
        long compressedData = counters.compressedData.get();
        long compressedDedupData = counters.compressedDedupData.get();
        long dedupData = counters.dedupData.get();
        long dedupCompressData = counters.dedupCompressData.get();

        System.err.printf("Running at %5.2f Kbps (%5.2f Gb/hour), %d/%d files in the read queue\n",
                (inputData - lastSize) * 1.0 / 1024 / POLL_INTERVAL_SEC,
                (inputData - lastSize) * 3600.0 / 1024 / 1024 / 1024 / POLL_INTERVAL_SEC,
                abq.size(),
                QUEUE_SIZE
        );

        lastSize = inputData;

        System.err.printf("COMPRESS:       %5.2fx increase, %d Kb --(block-compress)--> %d Kb\n",
                inputData * 1.0 / compressedData,
                inputData / 1024,
                compressedData / 1024
        );

        System.err.printf("COMPRESS+DEDUP: %5.2fx increase, %d Kb --(block-compress)--> %d Kb ------(dedup)-------> %d Kb\n",
                inputData * 1.0 / compressedDedupData,
                inputData / 1024,
                compressedData / 1024,
                compressedDedupData / 1024
        );

        System.err.printf("DEDUP:          %5.2fx increase, %d Kb ------(dedup)-------> %d Kb\n",
                inputData * 1.0 / dedupData,
                inputData / 1024,
                dedupData / 1024
        );

        System.err.printf("DEDUP+COMPRESS: %5.2fx increase, %d Kb ------(dedup)-------> %d Kb --(block-compress)--> %d Kb\n",
                inputData * 1.0 / dedupCompressData,
                inputData / 1024,
                dedupData / 1024,
                dedupCompressData / 1024
        );

        if (DETECT_COLLISIONS) {
            System.err.printf("detected collisions: %d on %s, %d on %s\n",
                    counters.collisions1.get() + counters.collisions1.get(),
                    ProcessTask.HASH_1,
                    counters.collisions2.get() + counters.collisions2.get(),
                    ProcessTask.HASH_2
            );
        }

        System.err.println();
    }


    private class PrintDetails implements Runnable {
        @Override
        public void run() {
            printProgress(true);
        }
    }
}
