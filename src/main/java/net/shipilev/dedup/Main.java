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
    private static final int THREADS = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors() + 1);
    private static final long POLL_INTERVAL_SEC = Integer.getInteger("pollInterval", 1);
    private static final boolean DETECT_COLLISIONS = Boolean.getBoolean("collisions");

    private final BlockingQueue<Runnable> abq = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final Counters counters = new Counters();

    private HashStorage uncompressedHashes;
    private HashStorage compressedHashes;

    private long firstPoll;

    public static void main(String[] args) throws NoSuchAlgorithmException, DigestException, InterruptedException, IOException {
        String path = ".";
        if (args.length > 0) {
            path = args[0];
        }

        new Main().run(path);
    }

    private void createStorages() {
        switch (STORAGE) {
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
                throw new IllegalStateException("Unknown storage " + Main.STORAGE);
        }
    }

    private void run(String path) throws InterruptedException, IOException {
        createStorages();

        System.err.println("Running with " + THREADS + " threads");
        System.err.println("Using " + BLOCK_SIZE + "-byte blocks");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(this::printProgress, POLL_INTERVAL_SEC, POLL_INTERVAL_SEC, TimeUnit.SECONDS);

        firstPoll = System.nanoTime();

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
                    File f = file.toFile();
                    counters.queuedData.addAndGet(f.length());
                    tpe.submit(new ProcessTask(BLOCK_SIZE, f, uncompressedHashes, compressedHashes, counters, DETECT_COLLISIONS));
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

        tpe.awaitTermination(365, TimeUnit.DAYS);
        executor.shutdownNow();

        System.err.println("FINAL RESULT:");
        System.err.println(path + ", using " + BLOCK_SIZE + "-byte blocks");
        printProgress();
    }

    private void printProgress() {
        long queuedData = counters.queuedData.get();
        long inputData = counters.inputData.get();
        long compressedData = counters.compressedData.get();
        long compressedDedupData = counters.compressedDedupData.get();
        long dedupData = counters.dedupData.get();
        long dedupCompressData = counters.dedupCompressData.get();

        final int M = 1024 * 1024;
        final int G = 1024 * 1024 * 1024;

        System.err.printf("Running at %5.2f MB/sec (%5.2f GB/hour), %d/%d files, %d/%d MB, ETA: %,ds\n",
                (inputData * 1.0 / M * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll),
                (inputData * 3600.0 / G * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll),
                abq.size(),
                QUEUE_SIZE,
                inputData / M,
                queuedData / M,
                TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - firstPoll) / inputData * (queuedData - inputData))
        );

        System.err.printf("COMPRESS:       %5.3fx increase, %,d MB --(block-compress)--> %,d MB\n",
                inputData * 1.0 / compressedData,
                inputData / M,
                compressedData / M
        );

        System.err.printf("COMPRESS+DEDUP: %5.3fx increase, %,d MB --(block-compress)--> %,d MB ------(dedup)-------> %,d MB\n",
                inputData * 1.0 / compressedDedupData,
                inputData / M,
                compressedData / M,
                compressedDedupData / M
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


}
