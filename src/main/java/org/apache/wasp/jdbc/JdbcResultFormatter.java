/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.wasp.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Format result utils.
 */
public class JdbcResultFormatter {

  // private static final ResourceBundle resourceBundle = ResourceBundle
  // .getBundle(JdbcResultFormatter.class.getName());

  private static final Object[] EMPTY_OBJ_ARRAY = new Object[0];

  // ~ Instance fields --------------------------------------------------------
  private OutputFile record = null;
  private StringBuilder sb = new StringBuilder();
  Connection conn;
  DatabaseMetaData meta;
  private Opts opts = new Opts(System.getProperties());

  public JdbcResultFormatter(Connection conn) throws SQLException {
    this.conn = conn;
    this.meta = conn.getMetaData();
  }

  /**
   * Format sql result.
   */
  public void format(Statement stmt, String sql) throws SQLException {
    ResultSet rs = stmt.executeQuery(sql);
    print(rs);
    // return sb.toString();
  }

  // /////////////////////////////////////
  // ResultSet output formatting classes
  // /////////////////////////////////////

  public int print(ResultSet rs) throws SQLException {
    OutputFormat f = new TableOutputFormat();
    Rows rows = new BufferedRows(rs);
    return f.print(rows);
  }

  /**
   * Issue the specified error message
   * 
   * @param msg
   *          the message to issue
   * 
   * @return false always
   */
  boolean error(String msg) {
    output(color().red(msg), true, System.err);
    return false;
  }

  boolean error(Throwable t) {
    handleException(t);
    return false;
  }

  void debug(String msg) {
    if (opts.getVerbose()) {
      output(color().blue(msg), true, System.err);
    }
  }

  static String loc(String res) {
    return loc(res, EMPTY_OBJ_ARRAY);
  }

  //
  // static String loc(String res, int param) {
  // try {
  // return MessageFormat.format(
  // new ChoiceFormat(resourceBundle.getString(res)).format(param),
  // new Object[] { new Integer(param) });
  // } catch (Exception e) {
  // return res + ": " + param;
  // }
  // }

  static String loc(String res, Object[] params) {
    // try {
    // return MessageFormat.format(resourceBundle.getString(res), params);
    // } catch (Exception e) {
    // e.printStackTrace();

      try {
        return res + ": " + Arrays.asList(params);
      } catch (Exception e2) {
        return res;
      }
    // }
  }

  // /////////////////////////////
  // Exception handling routines
  // /////////////////////////////

  void handleException(Throwable e) {
    while (e instanceof InvocationTargetException) {
      e = ((InvocationTargetException) e).getTargetException();
    }

    if (e instanceof SQLException) {
      handleSQLException((SQLException) e);
    } else if (!(opts.getVerbose())) { // all init errors
                                                          // must be verbose
      if (e.getMessage() == null) {
        error(e.getClass().getName());
      } else {
        error(e.getMessage());
      }
    } else {
      e.printStackTrace(System.err);
    }
  }

  void handleSQLException(SQLException e) {
    // all init errors must be verbose
    if ((e instanceof SQLWarning) && !(opts.getShowWarnings())) {
      return;
    }

    String type = (e instanceof SQLWarning) ? loc("Warning") : loc("Error");

    error(loc((e instanceof SQLWarning) ? "Warning" : "Error",
        new Object[] { (e.getMessage() == null) ? "" : e.getMessage().trim(),
            (e.getSQLState() == null) ? "" : e.getSQLState().trim(),
            new Integer(e.getErrorCode()) }));

    // all init errors must be verbose
    if (opts.getVerbose()) {
      e.printStackTrace();
    }

    // all init errors must be verbose
    if (!opts.getShowNestedErrs()) {
      return;
    }

    for (SQLException nested = e.getNextException(); (nested != null)
        && (nested != e); nested = nested.getNextException()) {
      handleSQLException(nested);
    }
  }

  static String getApplicationTitle() {
    Package pack = JdbcResultFormatter.class.getPackage();

    return loc(
        "app-introduction",
        new Object[] {
            "Wasp",
            pack.getImplementationVersion() == null ? "???" : pack
                .getImplementationVersion(), "Wasp", });
  }

  /**
   * Entry point to creating a {@link ColorBuffer} with color enabled or
   * disabled depending on the calue of {@link Opts#getColor}.
   */
  ColorBuffer color() {
    return new ColorBuffer(opts.getColor());
  }

  void output(ColorBuffer msg) {
    output(msg, true);
  }

  void output(ColorBuffer msg, boolean newline) {
    output(msg, newline, System.out);
  }

  void output(ColorBuffer msg, boolean newline, PrintStream out) {
    if (newline) {
      out.println(msg.getColor());
      sb.append(msg.getColor());
      sb.append("\n");
    } else {
      out.print(msg.getColor());
      sb.append(msg.getColor());
    }

    if (record == null) {
      return;
    }

    // only write to the record file if we are writing a line ...
    // otherwise we might get garbage from backspaces and such.
    if (newline) {
      record.addLine(msg.getMono()); // always just write mono
    }
  }

  public class OutputFile {
    final File file;
    final PrintWriter out;

    public OutputFile(String filename) throws IOException {
      file = new File(filename);
      out = new PrintWriter(new FileWriter(file));
    }

    public String toString() {
      return file.getAbsolutePath();
    }

    public void addLine(String command) {
      out.println(command);
    }

    public void close() throws IOException {
      out.close();
    }
  }

  static class Reflector {
    public static Object invoke(Object on, String method, Object[] args)
        throws InvocationTargetException, IllegalAccessException,
        ClassNotFoundException {
      return invoke(on, method, Arrays.asList(args));
    }

    public static Object invoke(Object on, String method, List args)
        throws InvocationTargetException, IllegalAccessException,
        ClassNotFoundException {
      return invoke(on, (on == null) ? null : on.getClass(), method, args);
    }

    public static Object invoke(Object on, Class defClass, String method,
        List args) throws InvocationTargetException, IllegalAccessException,
        ClassNotFoundException {
      Class c = (defClass != null) ? defClass : on.getClass();
      List candidateMethods = new LinkedList();

      Method[] m = c.getMethods();
      for (int i = 0; i < m.length; i++) {
        if (m[i].getName().equalsIgnoreCase(method)) {
          candidateMethods.add(m[i]);
        }
      }

      if (candidateMethods.size() == 0) {
        throw new IllegalArgumentException(loc("no-method", new Object[] {
            method, c.getName() }));
      }

      for (Iterator i = candidateMethods.iterator(); i.hasNext();) {
        Method meth = (Method) i.next();
        Class[] ptypes = meth.getParameterTypes();
        if (!(ptypes.length == args.size())) {
          continue;
        }

        Object[] converted = convert(args, ptypes);
        if (converted == null) {
          continue;
        }

        if (!Modifier.isPublic(meth.getModifiers())) {
          continue;
        }

        return meth.invoke(on, converted);
      }

      return null;
    }

    public static Object[] convert(List objects, Class[] toTypes)
        throws ClassNotFoundException {
      Object[] converted = new Object[objects.size()];
      for (int i = 0; i < converted.length; i++) {
        converted[i] = convert(objects.get(i), toTypes[i]);
      }
      return converted;
    }

    public static Object convert(Object ob, Class toType)
        throws ClassNotFoundException {
      if ((ob == null) || ob.toString().equals("null")) {
        return null;
      }

      if (toType == String.class) {
        return new String(ob.toString());
      } else if ((toType == Byte.class) || (toType == byte.class)) {
        return new Byte(ob.toString());
      } else if ((toType == Character.class) || (toType == char.class)) {
        return new Character(ob.toString().charAt(0));
      } else if ((toType == Short.class) || (toType == short.class)) {
        return new Short(ob.toString());
      } else if ((toType == Integer.class) || (toType == int.class)) {
        return new Integer(ob.toString());
      } else if ((toType == Long.class) || (toType == long.class)) {
        return new Long(ob.toString());
      } else if ((toType == Double.class) || (toType == double.class)) {
        return new Double(ob.toString());
      } else if ((toType == Float.class) || (toType == float.class)) {
        return new Float(ob.toString());
      } else if ((toType == Boolean.class) || (toType == boolean.class)) {
        return new Boolean(ob.toString().equals("true")
            || ob.toString().equals(true + "") || ob.toString().equals("1")
            || ob.toString().equals("on") || ob.toString().equals("yes"));
      } else if (toType == Class.class) {
        return Class.forName(ob.toString());
      }

      return null;
    }
  }

  class Opts {
    public static final int DEFAULT_MAX_WIDTH = 80;
    public static final int DEFAULT_MAX_HEIGHT = 80;
    public static final String PROPERTY_PREFIX = "sqlline.";
    public static final String PROPERTY_NAME_EXIT = PROPERTY_PREFIX
        + "system.exit";
    private boolean autosave = false;
    private boolean silent = false;
    private boolean color = false;
    private boolean showHeader = true;
    private int headerInterval = 100;
    private boolean fastConnect = true;
    private boolean autoCommit = true;
    private boolean verbose = false;
    private boolean force = false;
    private boolean incremental = false;
    private boolean showTime = true;
    private boolean showWarnings = true;
    private boolean showNestedErrs = false;
    private String numberFormat = "default";
    private int maxWidth = DEFAULT_MAX_WIDTH;
    private int maxHeight = DEFAULT_MAX_HEIGHT;
    private int maxColumnWidth = 15;
    private int rowLimit = 0;
    private int timeout = -1;
    private String isolation = "TRANSACTION_REPEATABLE_READ";
    private String outputFormat = "table";
    private boolean trimScripts = true;

    private File rcFile = new File(saveDir(), "sqlline.properties");
    private String historyFile = new File(saveDir(), "history")
        .getAbsolutePath();

    private String runFile;

    public Opts(Properties props) {
      loadProperties(props);
    }

    public String[] possibleSettingValues() {
      List vals = new LinkedList();
      vals.addAll(Arrays.asList(new String[] { "yes", "no", }));

      return (String[]) vals.toArray(new String[vals.size()]);
    }

    /**
     * The save directory if HOME/.sqlline/ on UNIX, and HOME/sqlline/ on
     * Windows.
     */
    public File saveDir() {
      String dir = System.getProperty("sqlline.rcfile");
      if ((dir != null) && (dir.length() > 0)) {
        return new File(dir);
      }

      File f = new File(System.getProperty("user.home"), ((System
          .getProperty("os.name").toLowerCase().indexOf("windows") != -1) ? ""
          : ".") + "sqlline").getAbsoluteFile();
      try {
        f.mkdirs();
      } catch (Exception e) {
      }

      return f;
    }

    public void save() throws IOException {
      OutputStream out = new FileOutputStream(rcFile);
      save(out);
      out.close();
    }

    public void save(OutputStream out) throws IOException {
      try {
        Properties props = toProperties();

        // don't save maxwidth: it is automatically set based on
        // the terminal configuration
        props.remove(PROPERTY_PREFIX + "maxwidth");

        props.store(out, getApplicationTitle());
      } catch (Exception e) {
        handleException(e);
      }
    }

    String[] propertyNames() throws IllegalAccessException,
        InvocationTargetException {
      TreeSet names = new TreeSet();

      // get all the values from getXXX methods
      Method[] m = getClass().getDeclaredMethods();
      for (int i = 0; (m != null) && (i < m.length); i++) {
        if (!(m[i].getName().startsWith("get"))) {
          continue;
        }

        if (m[i].getParameterTypes().length != 0) {
          continue;
        }

        String propName = m[i].getName().substring(3).toLowerCase();
        if (propName.equals("run")) {
          // Not a real property
          continue;
        }
        names.add(propName);
      }

      return (String[]) names.toArray(new String[names.size()]);
    }

    public Properties toProperties() throws IllegalAccessException,
        InvocationTargetException, ClassNotFoundException {
      Properties props = new Properties();

      String[] names = propertyNames();
      for (int i = 0; (names != null) && (i < names.length); i++) {
        props.setProperty(PROPERTY_PREFIX + names[i],
            Reflector.invoke(this, "get" + names[i], EMPTY_OBJ_ARRAY)
                .toString());
      }

      debug("properties: " + props.toString());
      return props;
    }

    public void load() throws IOException {
      InputStream in = new FileInputStream(rcFile);
      load(in);
      in.close();
    }

    public void load(InputStream fin) throws IOException {
      Properties p = new Properties();
      p.load(fin);
      loadProperties(p);
    }

    public void loadProperties(Properties props) {
      for (Iterator i = props.keySet().iterator(); i.hasNext();) {
        String key = i.next().toString();
        if (key.equals(PROPERTY_NAME_EXIT)) {
          // fix for sf.net bug 879422
          continue;
        }
        if (key.startsWith(PROPERTY_PREFIX)) {
          set(key.substring(PROPERTY_PREFIX.length()), props.getProperty(key));
        }
      }
    }

    public void set(String key, String value) {
      set(key, value, false);
    }

    public boolean set(String key, String value, boolean quiet) {
      try {
        Reflector.invoke(this, "set" + key, new Object[] { value });
        return true;
      } catch (Exception e) {
        if (!quiet) {
          error(loc("error-setting", new Object[] { key, e }));
        }
        return false;
      }
    }

    public void setFastConnect(boolean fastConnect) {
      this.fastConnect = fastConnect;
    }

    public boolean getFastConnect() {
      return this.fastConnect;
    }

    public void setAutoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit() {
      return this.autoCommit;
    }

    public void setVerbose(boolean verbose) {
      this.verbose = verbose;
    }

    public boolean getVerbose() {
      return this.verbose;
    }

    public void setShowTime(boolean showTime) {
      this.showTime = showTime;
    }

    public boolean getShowTime() {
      return this.showTime;
    }

    public void setShowWarnings(boolean showWarnings) {
      this.showWarnings = showWarnings;
    }

    public boolean getShowWarnings() {
      return this.showWarnings;
    }

    public void setShowNestedErrs(boolean showNestedErrs) {
      this.showNestedErrs = showNestedErrs;
    }

    public boolean getShowNestedErrs() {
      return this.showNestedErrs;
    }

    public void setNumberFormat(String numberFormat) {
      this.numberFormat = numberFormat;
    }

    public String getNumberFormat() {
      return this.numberFormat;
    }

    public void setMaxWidth(int maxWidth) {
      this.maxWidth = maxWidth;
    }

    public int getMaxWidth() {
      return this.maxWidth;
    }

    public void setMaxColumnWidth(int maxColumnWidth) {
      this.maxColumnWidth = maxColumnWidth;
    }

    public int getMaxColumnWidth() {
      return this.maxColumnWidth;
    }

    public void setRowLimit(int rowLimit) {
      this.rowLimit = rowLimit;
    }

    public int getRowLimit() {
      return this.rowLimit;
    }

    public void setTimeout(int timeout) {
      this.timeout = timeout;
    }

    public int getTimeout() {
      return this.timeout;
    }

    public void setIsolation(String isolation) {
      this.isolation = isolation;
    }

    public String getIsolation() {
      return this.isolation;
    }

    public void setHistoryFile(String historyFile) {
      this.historyFile = historyFile;
    }

    public String getHistoryFile() {
      return this.historyFile;
    }

    public void setColor(boolean color) {
      this.color = color;
    }

    public boolean getColor() {
      return this.color;
    }

    public void setShowHeader(boolean showHeader) {
      this.showHeader = showHeader;
    }

    public boolean getShowHeader() {
      return this.showHeader;
    }

    public void setHeaderInterval(int headerInterval) {
      this.headerInterval = headerInterval;
    }

    public int getHeaderInterval() {
      return this.headerInterval;
    }

    public void setForce(boolean force) {
      this.force = force;
    }

    public boolean getForce() {
      return this.force;
    }

    public void setIncremental(boolean incremental) {
      this.incremental = incremental;
    }

    public boolean getIncremental() {
      return this.incremental;
    }

    public void setSilent(boolean silent) {
      this.silent = silent;
    }

    public boolean getSilent() {
      return this.silent;
    }

    public void setAutosave(boolean autosave) {
      this.autosave = autosave;
    }

    public boolean getAutosave() {
      return this.autosave;
    }

    public void setOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
    }

    public String getOutputFormat() {
      return this.outputFormat;
    }

    public void setTrimScripts(boolean trimScripts) {
      this.trimScripts = trimScripts;
    }

    public boolean getTrimScripts() {
      return this.trimScripts;
    }

    public void setMaxHeight(int maxHeight) {
      this.maxHeight = maxHeight;
    }

    public int getMaxHeight() {
      return this.maxHeight;
    }

    public void setRun(String runFile) {
      this.runFile = runFile;
    }

    public String getRun() {
      return this.runFile;
    }
  }

  // ~ Inner Interfaces -------------------------------------------------------

  interface OutputFormat {
    int print(Rows rows);
  }

  // /////////////////////////////
  // Console interaction classes
  // /////////////////////////////

  /**
   * A buffer that can output segments using ANSI color.
   */
  final static class ColorBuffer implements Comparable {
    private static final ColorAttr BOLD = new ColorAttr("\033[1m");
    private static final ColorAttr NORMAL = new ColorAttr("\033[m");
    private static final ColorAttr REVERS = new ColorAttr("\033[7m");
    private static final ColorAttr LINED = new ColorAttr("\033[4m");
    private static final ColorAttr GREY = new ColorAttr("\033[1;30m");
    private static final ColorAttr RED = new ColorAttr("\033[1;31m");
    private static final ColorAttr GREEN = new ColorAttr("\033[1;32m");
    private static final ColorAttr BLUE = new ColorAttr("\033[1;34m");
    private static final ColorAttr CYAN = new ColorAttr("\033[1;36m");
    private static final ColorAttr YELLOW = new ColorAttr("\033[1;33m");
    private static final ColorAttr MAGENTA = new ColorAttr("\033[1;35m");
    private static final ColorAttr INVISIBLE = new ColorAttr("\033[8m");

    private final List parts = new LinkedList();

    private final boolean useColor;

    public ColorBuffer(boolean useColor) {
      this.useColor = useColor;

      append("");
    }

    public ColorBuffer(String str, boolean useColor) {
      this.useColor = useColor;

      append(str);
    }

    /**
     * Pad the specified String with spaces to the indicated length
     * 
     * @param str
     *          the String to pad
     * @param len
     *          the length we want the return String to be
     * 
     * @return the passed in String with spaces appended until the length
     *         matches the specified length.
     */
    ColorBuffer pad(ColorBuffer str, int len) {
      int n = str.getVisibleLength();
      while (n < len) {
        str.append(" ");
        n++;
      }

      return append(str);
    }

    ColorBuffer center(String str, int len) {
      StringBuffer buf = new StringBuffer(str);

      while (buf.length() < len) {
        buf.append(" ");

        if (buf.length() < len) {
          buf.insert(0, " ");
        }
      }

      return append(buf.toString());
    }

    ColorBuffer pad(String str, int len) {
      if (str == null) {
        str = "";
      }

      return pad(new ColorBuffer(str, false), len);
    }

    public String getColor() {
      return getBuffer(useColor);
    }

    public String getMono() {
      return getBuffer(false);
    }

    String getBuffer(boolean color) {
      StringBuffer buf = new StringBuffer();
      for (Iterator i = parts.iterator(); i.hasNext();) {
        Object next = i.next();
        if (!color && (next instanceof ColorAttr)) {
          continue;
        }

        buf.append(next.toString());
      }

      return buf.toString();
    }

    /**
     * Truncate the ColorBuffer to the specified length and return the new
     * ColorBuffer. Any open color tags will be closed.
     */
    public ColorBuffer truncate(int len) {
      ColorBuffer cbuff = new ColorBuffer(useColor);
      ColorAttr lastAttr = null;
      for (Iterator i = parts.iterator(); (cbuff.getVisibleLength() < len)
          && i.hasNext();) {
        Object next = i.next();
        if (next instanceof ColorAttr) {
          lastAttr = (ColorAttr) next;
          cbuff.append((ColorAttr) next);
          continue;
        }

        String val = next.toString();
        if ((cbuff.getVisibleLength() + val.length()) > len) {
          int partLen = len - cbuff.getVisibleLength();
          val = val.substring(0, partLen);
        }

        cbuff.append(val);
      }

      // close off the buffer with a normal tag
      if ((lastAttr != null) && (lastAttr != NORMAL)) {
        cbuff.append(NORMAL);
      }

      return cbuff;
    }

    public String toString() {
      return getColor();
    }

    public ColorBuffer append(String str) {
      parts.add(str);
      return this;
    }

    public ColorBuffer append(ColorBuffer buf) {
      parts.addAll(buf.parts);
      return this;
    }

    public ColorBuffer append(ColorAttr attr) {
      parts.add(attr);
      return this;
    }

    public int getVisibleLength() {
      return getMono().length();
    }

    public ColorBuffer append(ColorAttr attr, String val) {
      parts.add(attr);
      parts.add(val);
      parts.add(NORMAL);
      return this;
    }

    public ColorBuffer bold(String str) {
      return append(BOLD, str);
    }

    public ColorBuffer lined(String str) {
      return append(LINED, str);
    }

    public ColorBuffer grey(String str) {
      return append(GREY, str);
    }

    public ColorBuffer red(String str) {
      return append(RED, str);
    }

    public ColorBuffer blue(String str) {
      return append(BLUE, str);
    }

    public ColorBuffer green(String str) {
      return append(GREEN, str);
    }

    public ColorBuffer cyan(String str) {
      return append(CYAN, str);
    }

    public ColorBuffer yellow(String str) {
      return append(YELLOW, str);
    }

    public ColorBuffer magenta(String str) {
      return append(MAGENTA, str);
    }

    public int compareTo(Object other) {
      return getMono().compareTo(((ColorBuffer) other).getMono());
    }

    private static class ColorAttr {
      private final String attr;

      public ColorAttr(String attr) {
        this.attr = attr;
      }

      public String toString() {
        return attr;
      }
    }
  }

  /**
   * OutputFormat for a pretty, table-like format.
   */
  class TableOutputFormat implements OutputFormat {
    public int print(Rows rows) {
      int index = 0;
      ColorBuffer header = null;
      ColorBuffer headerCols = null;
      final int width = opts.getMaxWidth() - 4;

      // normalize the columns sizes
      rows.normalizeWidths();

      for (; rows.hasNext();) {
        Rows.Row row = (Rows.Row) rows.next();
        ColorBuffer cbuf = getOutputString(rows, row);
        cbuf = cbuf.truncate(width);

        if (index == 0) {
          StringBuffer h = new StringBuffer();
          for (int j = 0; j < row.sizes.length; j++) {
            for (int k = 0; k < row.sizes[j]; k++) {
              h.append('-');
            }
            h.append("-+-");
          }

          headerCols = cbuf;
          header = color().green(h.toString()).truncate(
              headerCols.getVisibleLength());
        }

        if ((index == 0)
            || ((opts.getHeaderInterval() > 0)
                && ((index % opts.getHeaderInterval()) == 0) && opts
                  .getShowHeader())) {
          printRow(header, true);
          printRow(headerCols, false);
          printRow(header, true);
        }

        if (index != 0) { // don't output the header twice
          printRow(cbuf, false);
        }

        index++;
      }

      if ((header != null) && opts.getShowHeader()) {
        printRow(header, true);
      }

      return index - 1;
    }

    void printRow(ColorBuffer cbuff, boolean header) {
      if (header) {
        output(color().green("+-").append(cbuff).green("-+"));
      } else {
        output(color().green("| ").append(cbuff).green(" |"));
      }
    }

    public ColorBuffer getOutputString(Rows rows, Rows.Row row) {
      return getOutputString(rows, row, " | ");
    }

    ColorBuffer getOutputString(Rows rows, Rows.Row row, String delim) {
      ColorBuffer buf = color();

      for (int i = 0; i < row.values.length; i++) {
        if (buf.getVisibleLength() > 0) {
          buf.green(delim);
        }

        ColorBuffer v;

        if (row.isMeta) {
          v = color().center(row.values[i], row.sizes[i]);
          if (rows.isPrimaryKey(i)) {
            buf.cyan(v.getMono());
          } else {
            buf.bold(v.getMono());
          }
        } else {
          v = color().pad(row.values[i], row.sizes[i]);
          if (rows.isPrimaryKey(i)) {
            buf.cyan(v.getMono());
          } else {
            buf.append(v.getMono());
          }
        }
      }

      if (row.deleted) { // make deleted rows red
        buf = color().red(buf.getMono());
      } else if (row.updated) { // make updated rows blue
        buf = color().blue(buf.getMono());
      } else if (row.inserted) { // make new rows green
        buf = color().green(buf.getMono());
      }

      return buf;
    }
  }

  /**
   * Abstract base class representing a set of rows to be displayed.
   */
  abstract class Rows implements Iterator {
    final ResultSetMetaData rsMeta;
    final Boolean[] primaryKeys;
    final NumberFormat numberFormat;

    Rows(ResultSet rs) throws SQLException {
      rsMeta = rs.getMetaData();
      int count = rsMeta.getColumnCount();
      primaryKeys = new Boolean[count];
      if (opts.getNumberFormat().equals("default")) {
        numberFormat = null;
      } else {
        numberFormat = new DecimalFormat(opts.getNumberFormat());
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Update all of the rows to have the same size, set to the maximum length
     * of each column in the Rows.
     */
    abstract void normalizeWidths();

    /**
     * Return whether the specified column (0-based index) is a primary key.
     * Since this method depends on whether the JDBC driver property implements
     * {@link ResultSetMetaData#getTableName} (many do not), it is not reliable
     * for all databases.
     */
    boolean isPrimaryKey(int col) {
      if (primaryKeys[col] != null) {
        return primaryKeys[col].booleanValue();
      }

      try {
        // this doesn't always work, since some JDBC drivers (e.g.,
        // Oracle's) return a blank string from getTableName.
        String table = rsMeta.getTableName(col + 1);
        String column = rsMeta.getColumnName(col + 1);

        if ((table == null) || (table.length() == 0) || (column == null)
            || (column.length() == 0)) {
          return (primaryKeys[col] = new Boolean(false)).booleanValue();
        }

        ResultSet pks = meta.getPrimaryKeys(meta.getConnection()
            .getCatalog(), null, table);

        try {
          while (pks.next()) {
            if (column.equalsIgnoreCase(pks.getString("COLUMN_NAME"))) {
              return (primaryKeys[col] = new Boolean(true)).booleanValue();
            }
          }
        } finally {
          pks.close();
        }

        return (primaryKeys[col] = new Boolean(false)).booleanValue();
      } catch (SQLException sqle) {
        return (primaryKeys[col] = new Boolean(false)).booleanValue();
      }
    }

    class Row {
      final String[] values;
      final boolean isMeta;
      private boolean deleted;
      private boolean inserted;
      private boolean updated;
      private int[] sizes;

      Row(int size) throws SQLException {
        isMeta = true;
        values = new String[size];
        sizes = new int[size];
        for (int i = 0; i < size; i++) {
          values[i] = rsMeta.getColumnLabel(i + 1);
          sizes[i] = (values[i] == null) ? 1 : values[i].length();
        }

        deleted = false;
        updated = false;
        inserted = false;
      }

      Row(int size, ResultSet rs) throws SQLException {
        isMeta = false;
        values = new String[size];
        sizes = new int[size];

        try {
          deleted = rs.rowDeleted();
        } catch (Throwable t) {
        }
        try {
          updated = rs.rowUpdated();
        } catch (Throwable t) {
        }
        try {
          inserted = rs.rowInserted();
        } catch (Throwable t) {
        }

        for (int i = 0; i < size; i++) {
          if (numberFormat != null) {
            Object o = rs.getObject(i + 1);
            if (o == null) {
              values[i] = null;
            } else if (o instanceof Number) {
              values[i] = numberFormat.format(o);
            } else {
              values[i] = o.toString();
            }
          } else {
            values[i] = String.valueOf(rs.getObject(i + 1));
          }
          sizes[i] = (values[i] == null) ? 1 : values[i].length();
        }
      }
    }
  }

  /**
   * Rows implementation which buffers all rows in a linked list.
   */
  class BufferedRows extends Rows {
    private final LinkedList list;

    private final Iterator iterator;

    BufferedRows(ResultSet rs) throws SQLException {
      super(rs);

      list = new LinkedList();

      int count = rsMeta.getColumnCount();

      list.add(new Row(count));

      while (rs.next()) {
        list.add(new Row(count, rs));
      }

      iterator = list.iterator();
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public Object next() {
      return iterator.next();
    }

    void normalizeWidths() {
      int[] max = null;
      for (int i = 0; i < list.size(); i++) {
        Row row = (Row) list.get(i);
        if (max == null) {
          max = new int[row.values.length];
        }

        for (int j = 0; j < max.length; j++) {
          max[j] = Math.max(max[j], row.sizes[j] + 1);
        }
      }

      for (int i = 0; i < list.size(); i++) {
        Row row = (Row) list.get(i);
        row.sizes = max;
      }
    }
  }

}

