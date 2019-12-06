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

    static final String STORAGE = System.getProperty("storage", "inmemory");
    static final String HASH = System.getProperty("hash", "SHA-256");
    static final int QUEUE_SIZE = Integer.getInteger("queueSize", 1000 * 1000);
    static final int BLOCK_SIZE = Integer.getInteger("blockSize", 4096);
    static final int THREADS = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors() - 1);
    static final long POLL_INTERVAL_SEC = Integer.getInteger("pollInterval", 1);

    private final BlockingQueue<Runnable> abq = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final Counters counters = new Counters();

    private HashStorage hashes;

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
                if (attrs.isRegularFile() && !attrs.isSymbolicLink()) {
                    counters.queuedData.addAndGet(Files.size(file));
                    counters.queuedFiles.incrementAndGet();
                    tpe.submit(new ProcessTask(file, hashes, counters));
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

        final int M = 1024 * 1024;
        final int G = 1024 * 1024 * 1024;

        System.err.printf("Running at %5.2f MB/sec (%5.2f GB/hour), %d/%d files, %d/%d MB, ETA: %,ds\n",
                (inputData * 1.0 / M * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll + 1),
                (inputData * 3600.0 / G * TimeUnit.SECONDS.toNanos(1)) / (System.nanoTime() - firstPoll + 1),
                processedFiles,
                queuedFiles,
                inputData / M,
                queuedData / M,
                TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - firstPoll) / inputData * (queuedData - inputData))
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
