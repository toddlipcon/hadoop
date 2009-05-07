package com.cloudera.mysqloutput;

import junit.framework.TestCase;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import java.util.ArrayList;
import java.util.List;

public class TestMySQLOutputFormat extends TestCase {
  private static JobConf defaultConf = new JobConf();

  private static FileSystem localFs = null;
  static {
    try {
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init fail", e);
    }
  }

  private static String attempt = "attempt_200707121733_0001_m_000000_0";

  private static Path workDir = 
    new Path(new Path(
                      new Path(System.getProperty("test.build.data", "."), 
                               "data"), 
                      FileOutputCommitter.TEMP_DIR_NAME), "_" + attempt);


  private Schema getTestSchema() {
    List<Schema.Column> cols = new ArrayList<Schema.Column>();
    cols.add(new Schema.Column("column_a",
                               "varchar(10) not null"));
    cols.add(new Schema.Column("column_b",
                               "varchar(20) not null"));
    return new Schema(cols);
  }

  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    job.set("mapred.task.id", attempt);

    Reporter reporter = Reporter.NULL;

    String file = "test.name";

    localFs.delete(workDir, true);
    if (!localFs.mkdirs(workDir)) {
      fail("Failed to create output directory");
    }    


    FileOutputFormat.setOutputPath(job, workDir.getParent().getParent());
    job.set("mapred.work.output.dir", workDir.toString());

    MySQLOutputFormat of = new MySQLOutputFormat();
    RecordWriter rw =
      of.getRecordWriter(localFs, job, file, reporter);

    NullWritable nw = NullWritable.get();
    Schema schema = getTestSchema();
    MySQLRow row = new MySQLRow(schema);

    try {
      row.setFields(new Object[] {"hello", "world"});
      rw.write(nw, row);
      row.setFields(new Object[] {"goodbye", "now!"});
      rw.write(nw, row);
    } finally {
      rw.close(reporter);
    }

  }

  public static void main(String[] args) throws Exception {
    new TestMySQLOutputFormat().testFormat();
  }
}