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

import com.mchange.v2.c3p0.DataSources;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class H2HashStorage implements HashStorage {
    private final DataSource datasource;

    public H2HashStorage(String dbName) {
        try {
            Class.forName("org.h2.Driver").newInstance();

            DataSource ds = DataSources.unpooledDataSource(
                    "jdbc:h2:" + dbName + ";create=true;MULTI_THREADED=1;COMPRESS_LOB=NO;MAX_LENGTH_INPLACE_LOB=256;CACHE_SIZE=512000",
                    "app",
                    "");
            datasource = DataSources.pooledDataSource(ds);

            Statement statement = datasource.getConnection().createStatement();
            statement.execute("CREATE TABLE hashes(hash BINARY(256))");
            statement.execute("CREATE UNIQUE INDEX hashI ON hashes(hash)");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        System.err.println("Using H2 datastorage @ " + dbName);
    }

    @Override
    public boolean add(byte[] data) {
        try (Connection connection = datasource.getConnection()) {
            PreparedStatement insertStmt = connection.prepareStatement("INSERT INTO hashes(hash) VALUES(?)");
            insertStmt.setBytes(1, data);
            insertStmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
