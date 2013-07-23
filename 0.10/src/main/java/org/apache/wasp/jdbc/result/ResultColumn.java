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
package org.apache.wasp.jdbc.result;

import org.apache.wasp.DataType;
import org.apache.wasp.FieldKeyWord;
import org.apache.wasp.meta.Field;

import java.io.IOException;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {

  /**
   * The column name or null.
   */
  final String columnName;

  final String family;

  final DataType dataType;

  /**
   * The precision.
   */
  final long precision;

  /**
   * The scale.
   */
  final int scale;

  /**
   * True if this column is nullable.
   */
  final int nullable;

  /**
   * Read an object from the given transfer object.
   * 
   * @param field
   *          the Field from FTable
   */
  ResultColumn(Field field) throws IOException {
    family = field.getFamily();
    dataType = field.getType();
    columnName = field.getName();
    precision = Long.MAX_VALUE;
    scale = Integer.MAX_VALUE;
    FieldKeyWord keyWord = field.getKeyWord();
    switch (keyWord) {
      case REQUIRED : nullable = 1; break;
      default: nullable = 0;
    }
  }
}