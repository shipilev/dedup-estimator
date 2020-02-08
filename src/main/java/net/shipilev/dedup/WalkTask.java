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

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class WalkTask extends RecursiveAction {
    private final Path dir;
    private final HashStorage hashes;
    private final Counters counters;

    public WalkTask(Path dir, HashStorage hashes, Counters counters) {
        this.dir = dir;
        this.hashes = hashes;
        this.counters = counters;
    }

    @Override
    protected void compute() {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            List<ForkJoinTask<?>> tasks = new ArrayList<>();
            for (Path p : ds) {
                BasicFileAttributes bfa = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (bfa.isSymbolicLink()) {
                    continue;
                }
                if (bfa.isDirectory()) {
                    counters.queuedDirs.incrementAndGet();
                    tasks.add(new WalkTask(p, hashes, counters));
                }
                if (bfa.isRegularFile()) {
                    counters.queuedData.addAndGet(bfa.size());
                    counters.queuedFiles.incrementAndGet();
                    tasks.add(new ProcessTask(p, hashes, counters));
                }
            }
            ForkJoinTask.invokeAll(tasks);
            counters.processedDirs.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
