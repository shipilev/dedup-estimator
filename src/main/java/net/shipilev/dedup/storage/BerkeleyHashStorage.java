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
package net.shipilev.dedup.storage;

import com.sleepycat.je.*;

import java.io.File;

public class BerkeleyHashStorage implements HashStorage {
    private final Database database;
    private final DatabaseEntry constValue;

    public BerkeleyHashStorage(String name) {
        try {
            EnvironmentConfig config = new EnvironmentConfig();
            config.setAllowCreate(true);

            File file = new File(name);
            boolean isCreated = file.mkdirs();
            if (!isCreated) {
                System.err.println("WARNING: " + name + " already exists, did you forget to remove previous DB?\n" +
                        "WARNING: This might interfere with your results. Please proceed only if you know what you're doing.");
            }
            Environment environment = new Environment(file, config);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);

            database = environment.openDatabase(null, name, dbConfig);
            constValue = new DatabaseEntry(new byte[1]);
        } catch (DatabaseException e) {
            throw new IllegalStateException(e);
        }
        System.err.println("Using BerkeleyDB datastorage @ " + name);
    }

    @Override
    public boolean add(byte[] data) {
        try {
            OperationStatus status = database.putNoOverwrite(null, new DatabaseEntry(data), constValue);
            return status.equals(OperationStatus.SUCCESS);
        } catch (DatabaseException e) {
            e.printStackTrace();
            return false;
        }
    }
}
