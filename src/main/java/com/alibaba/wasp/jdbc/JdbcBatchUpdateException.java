/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package com.alibaba.wasp.jdbc;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.BatchUpdateException;
import java.sql.SQLException;

/**
 * Represents a batch update database exception.
 */
public class JdbcBatchUpdateException extends BatchUpdateException {

    private static final long serialVersionUID = 1L;

    /**
     * INTERNAL
     */
    JdbcBatchUpdateException(SQLException next, int[] updateCounts) {
        super(next.getMessage(), next.getSQLState(), next.getErrorCode(), updateCounts);
        setNextException(next);
    }

    /**
     * INTERNAL
     */
    public void printStackTrace() {
        // The default implementation already does that,
        // but we do it again to avoid problems.
        // If it is not implemented, somebody might implement it
        // later on which would be a problem if done in the wrong way.
        printStackTrace(System.err);
    }

    /**
     * INTERNAL
     */
    public void printStackTrace(PrintWriter s) {
        if (s != null) {
            super.printStackTrace(s);
            if (getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

    /**
     * INTERNAL
     */
    public void printStackTrace(PrintStream s) {
        if (s != null) {
            super.printStackTrace(s);
            if (getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

}
