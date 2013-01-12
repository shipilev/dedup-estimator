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

    private static final int QUEUE_SIZE = 10 * 1000;
    private static final long POLL_INTERVAL_SEC = 1;

    private static String path;
    private static String storage;
    private static Integer blockSize;

    private HashStorage uncompressedHashes;
    private HashStorage compressedHashes;

    private Counters counters = new Counters();

    private int printCounter;
    private long lastSize;
    private BlockingQueue<Runnable> abq = new ArrayBlockingQueue<>(QUEUE_SIZE);

    public static void main(String[] args) throws NoSuchAlgorithmException, DigestException, InterruptedException, IOException {

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

    private void run() throws InterruptedException, IOException {
        createStorages(storage);

        System.err.println("Using " + blockSize + "-byte blocks");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new PrintDetails(), 1, POLL_INTERVAL_SEC, TimeUnit.SECONDS);


        final ThreadPoolExecutor tpe = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), 1, TimeUnit.DAYS, abq);
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        Files.walkFileTree(new File(path).toPath(), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isSymbolicLink()) {
                    tpe.submit(new ProcessTask(blockSize, file.toFile(), uncompressedHashes, compressedHashes, counters));
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
    }


    private void printProgress(boolean proceedAlways) {
        printCounter++;
        if (!proceedAlways && (printCounter & (16384 - 1)) == 0) {
            return;
        }

        System.err.printf("Running at %5.2f Kbps (%5.2f Gb/hour), %d/%d files in read queue\n",
                (counters.inputData.get() - lastSize) * 1.0 / 1024 / POLL_INTERVAL_SEC,
                (counters.inputData.get() - lastSize) * 3600.0 / 1024 / 1024 / 1024 / POLL_INTERVAL_SEC,
                abq.size(),
                QUEUE_SIZE
        );

        lastSize = counters.inputData.get();

        System.err.printf("COMPRESS:       %5.2fx increase. %d Kb --(compress)--> %d Kb\n",
                counters.inputData.get() * 1.0 / counters.compressedData.get(),
                counters.inputData.get() / 1024,
                counters.compressedData.get() / 1024
        );

        System.err.printf("COMPRESS+DEDUP: %5.2fx increase. %d Kb --(compress)--> %d Kb ------(dedup)-------> %d Kb\n",
                counters.inputData.get() * 1.0 / (counters.compressedDedupData.get()),
                counters.inputData.get() / 1024,
                counters.compressedData.get() / 1024,
                counters.compressedDedupData.get() / 1024
        );

        System.err.printf("DEDUP:          %5.2fx increase, %d Kb ----(dedup)---> %d Kb\n",
                counters.inputData.get() * 1.0 / counters.dedupData.get(),
                counters.inputData.get() / 1024,
                counters.dedupData.get() / 1024
        );

        System.err.printf("DEDUP+COMPRESS: %5.2fx increase. %d Kb ----(dedup)---> %d Kb --(block-compress)--> %d Kb\n",
                counters.inputData.get() * 1.0 / counters.dedupCompressData.get(),
                counters.inputData.get() / 1024,
                counters.dedupData.get() / 1024,
                counters.dedupCompressData.get() / 1024
        );

        System.err.printf("detected collisions: %d on %s, %d on %s\n",
                counters.collisions1.get() + counters.collisions1.get(),
                ProcessTask.HASH_1,
                counters.collisions2.get() + counters.collisions2.get(),
                ProcessTask.HASH_2
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
