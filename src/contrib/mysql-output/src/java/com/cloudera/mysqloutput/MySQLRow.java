package com.cloudera.mysqloutput;

import java.io.OutputStream;
import java.io.IOException;

public class MySQLRow {
  private Schema schema;
  private Object[] vals;

  public MySQLRow(Schema schema) {
    this.schema = schema;
  }

  public void setFields(Object[] vals) {
    this.vals = vals;
  }

  Schema getSchema() {
    return schema;
  }

  void write(OutputStream os, byte fieldTerminator) throws IOException {
    if (vals == null) {
      throw new IOException("No vals set in this row!");
    }

    schema.write(os, vals, fieldTerminator);
  }
}