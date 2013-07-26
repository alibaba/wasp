/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.wasp.jdbc.command;

import com.alibaba.wasp.jdbc.expression.ParameterInterface;
import com.alibaba.wasp.jdbc.result.ResultInterface;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Represents a SQL statement.
 */
public interface CommandInterface {

    /**
     * The type for unknown statement.
     */
    int UNKNOWN = 0;

    /**
     * The type of a DELETE statement.
     */
    int DELETE = 1;

    /**
     * The type of a INSERT statement.
     */
    int INSERT = 2;

    /**
     * The type of a SELECT statement.
     */
    int SELECT = 3;

    /**
     * The type of a UPDATE statement.
     */
    int UPDATE = 4;

    /**
     * Get command type.
     *
     * @return one of the constants above
     */
    int getCommandType();

    /**
     * Check if this is a query.
     *
     * @return true if it is a query
     */
    boolean isQuery();

    /**
     * Get the parameters (if any).
     *
     * @return the parameters
     */
    ArrayList<? extends ParameterInterface> getParameters();

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @return the result
     */
    ResultInterface executeQuery(int maxRows) throws SQLException;

    /**
     * Execute the statement
     *
     * @return the update count
     */
    int executeUpdate() throws SQLException;

    /**
     * Close the statement.
     */
    void close();

    /**
     * Cancel the statement if it is still processing.
     */
    void cancel();

    /**
     * Get an empty result set containing the meta data of the result.
     *
     * @return the empty result
     */
    ResultInterface getMetaData();
}
