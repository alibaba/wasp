package org.apache.wasp;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Thrown if a request is table schema modification is requested but made for an
 * invalid column name.
 */
public class InvalidColumnOperationException extends DoNotRetryIOException {
  private static final long serialVersionUID = -872511639311444777L;

  /** default constructor */
  public InvalidColumnOperationException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public InvalidColumnOperationException(String s) {
    super(s);
  }
}