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
package org.apache.wasp.meta;

/**
 * Some of the attributes of the index column.
 * 
 */
public class IndexField {
  private String family;
  private String name;
  private byte[] value;
  private boolean isNumeric;

  /**
   * Default constructor.
   * 
   * @param family
   * @param name
   * @param value
   * @param isNumeric
   */
  public IndexField(String family, String name, byte[] value, boolean isNumeric) {
    this.family = family;
    this.name = name;
    this.value = value;
    this.isNumeric = isNumeric;
  }

  public IndexField(String name, byte[] value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Return column name.
   * 
   * @return column name.
   */
  public String getName() {
    return name;
  }

  /**
   * Return current value.
   * 
   * @return current value.
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * Return column family.
   * 
   * @return column family.
   */
  public String getFamily() {
    return family;
  }

  /**
   * This field is numeric or not.
   * 
   * @return boolean
   */
  public boolean isNumeric() {
    return isNumeric;
  }

  /**
   * @param family the family to set
   */
  public void setFamily(String family) {
    this.family = family;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @param value the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  /**
   * @param isNumeric the isNumeric to set
   */
  public void setNumeric(boolean isNumeric) {
    this.isNumeric = isNumeric;
  }
}