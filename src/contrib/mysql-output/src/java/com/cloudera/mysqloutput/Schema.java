package com.cloudera.mysqloutput;

import java.util.List;
import java.util.ArrayList;
import java.io.OutputStream;
import java.io.IOException;
import java.io.DataInput;

public class Schema {
  private final List<Column> columns;

  public Schema(List<Column> columns) {
    this.columns = new ArrayList<Column>();
    this.columns.addAll(columns);
  }

  public String getCreateTableStatement(String tableName, String engine) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE " + tableName + " (");
    boolean first = true;
    for (Column col : columns) {
      if (! first) {
        sb.append(",");
      }
      first = false;
      sb.append("\n");

      sb.append(col.getDdl());
    }

    sb.append(") ENGINE=" + engine + ";\n");
    return sb.toString();
  }

  public void write(OutputStream os, Object[] vals, byte fieldTerminator) throws IOException {
    if (vals.length != columns.size()) {
      throw new IOException("Column length mismatch. Expected " + columns.size() + " got " +
                            vals.length);
    }

    int i = 0;
    for (Column col : columns) {
      Object val = vals[i];

      col.serializeForSql(os, val);
      os.write(fieldTerminator);
    }
  }

  public static class Column {
    public final String name;
    public final String mysqlDescription;
    public final ColumnWriter writer;

    public Column(String name, String mysqlDescription) {
      this.name = name;
      this.mysqlDescription = mysqlDescription;
      this.writer = new StringifyWriter();
    }

    public Column(String name, String mysqlDescription,
                  ColumnWriter writer) {
      this.name = name;
      this.mysqlDescription = mysqlDescription;
      this.writer = writer;
    }

    public void serializeForSql(OutputStream os, Object value) throws IOException {
      this.writer.serializeForSql(os, value);
    }

    public String getDdl() {
      return "`" + name + "` " + mysqlDescription;
    }
  }

  public interface ColumnWriter {
    public abstract void serializeForSql(OutputStream os,
                                         Object value) throws IOException;
  }

  public static class StringifyWriter implements ColumnWriter {
    public void serializeForSql(OutputStream os,
                                Object val) throws IOException {
      os.write(String.valueOf(val).getBytes("UTF-8"));
    }
  }
}