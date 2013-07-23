package org.apache.wasp.meta;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.FConstants;
import org.apache.wasp.plan.parser.Condition;
import org.apache.wasp.plan.parser.QueryInfo;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.util.New;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;

public class RowBuilderTestUtil {

  public static Pair<Index, List<IndexField>> buildTestIndex() {
    String indexName = "TEST_INDEX";
    List<IndexField> indexFields = New.arrayList();

    Index index = new Index();
    index.setIndexName(indexName);
    LinkedHashMap<String, Field> indexKeys = new LinkedHashMap<String, Field>();

    Field index1 = new Field();
    index1.setType(DataType.STRING);
    index1.setName("index1");
    index1.setFamily("cf");
    IndexField indexField1 = new IndexField("index1", Bytes.toBytes("test"));

    Field index2 = new Field();
    index2.setType(DataType.INT32);
    index2.setName("index2");
    index2.setFamily("cf");
    IndexField indexField2 = new IndexField("index2", Bytes.toBytes(11));

    Field index3 = new Field();
    index3.setType(DataType.INT64);
    index3.setName("index3");
    index3.setFamily("cf");
    IndexField indexField3 = new IndexField("index3", Bytes.toBytes(199));

    indexKeys.put(index1.getName(), index1);
    indexKeys.put(index2.getName(), index2);
    indexKeys.put(index3.getName(), index3);

    indexFields.add(indexField1);
    indexFields.add(indexField2);
    indexFields.add(indexField3);

    index.setIndexKeys(indexKeys);
    return new Pair<Index, List<IndexField>>(index, indexFields);
  }

  public static NavigableMap<byte[], NavigableMap<byte[], byte[]>> buildValues() {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> values;
    values = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
        Bytes.BYTES_RAWCOMPARATOR);
    NavigableMap<byte[], byte[]> colValues = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_RAWCOMPARATOR);

    colValues.put(Bytes.toBytes("index1"), Bytes.toBytes("test"));
    colValues.put(Bytes.toBytes("index2"), Bytes.toBytes(11));
    colValues.put(Bytes.toBytes("index3"), Bytes.toBytes(199));
    values.put(Bytes.toBytes("cf"), colValues);
    return values;
  }

  public static List<String> buildIndexNames(String indexName) {
    List<String> indexs;
    indexs = New.arrayList();
    indexs.add(FConstants.WASP_TABLE_INDEX_PREFIX + null
        + FConstants.TABLE_ROW_SEP + indexName);
    return indexs;
  }

  public static QueryInfo buildQueryInfo() throws UnsupportedException {
    LinkedHashMap<String, Condition> fieldValue = new LinkedHashMap<String, Condition>();

    // field name - data type - value

    // index1 - String - =test
    Condition index1Condition = new Condition("index1",
        Condition.ConditionType.EQUAL, new SQLCharExpr("test"));

    // index2 - int - =11
    Condition index2Condition = new Condition("index2",
        Condition.ConditionType.EQUAL, new SQLIntegerExpr(11));

    // index3 - long - <=199
    Condition index3Condition = new Condition("index3",
        Condition.ConditionType.RANGE, new SQLNumberExpr(199),
        SQLBinaryOperator.LessThanOrEqual);

    fieldValue.put(index1Condition.getFieldName(), index1Condition);
    fieldValue.put(index2Condition.getFieldName(), index2Condition);

    List<Condition> ranges = new ArrayList<Condition>();
    ranges.add(index3Condition);
    return new QueryInfo(QueryInfo.QueryType.SCAN, fieldValue, ranges);
  }
}