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

import java.sql.*;

public class DerbyHashStorage implements HashStorage {
    private Connection connection;
    private PreparedStatement insertStmt;

    public DerbyHashStorage(String dbName) {
        try {
            create(dbName);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        System.err.println("Using Derby datastorage @ " + dbName);
    }

    private void create(String dbName) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        connection = DriverManager.getConnection("jdbc:derby:" + dbName + ";create=true");

        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE hashes(hash CHAR(254) FOR BIT DATA)");
        statement.execute("CREATE UNIQUE INDEX hashI ON hashes(hash)");

        insertStmt = connection.prepareStatement("INSERT INTO hashes(hash) VALUES(?)");
    }

    @Override
    public boolean add(byte[] data) {
        try {
            insertStmt.setBytes(1, data);
            insertStmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
