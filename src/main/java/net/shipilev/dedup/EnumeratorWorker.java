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

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;

public class EnumeratorWorker implements Runnable {
    private final File root;
    private final BlockingQueue<File> processQueue;

    public EnumeratorWorker(File root, BlockingQueue<File> resultQueue) {
        this.root = root;
        this.processQueue = resultQueue;
    }

    @Override
    public void run() {
        try {
            Files.walkFileTree(root.toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!attrs.isSymbolicLink()) {
                        try {
                            processQueue.put(file.toFile());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return FileVisitResult.TERMINATE;
                        }
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
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
