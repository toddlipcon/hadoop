package com.cloudera.mysqloutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileOutputStream;

public class MySQLOutputFormat extends FileOutputFormat <NullWritable, MySQLRow> {
  private static final Log LOG = LogFactory.getLog(MySQLOutputFormat.class);

  /************************************************************/

  public RecordWriter<NullWritable, MySQLRow> getRecordWriter(
    FileSystem fs, JobConf job, String name, Progressable progress)
    throws IOException
  {
    LOG.info("creating rw");

    Path workDir = getWorkOutputPath(job);
    FileSystem workFs = workDir.getFileSystem(job);

    Path resultDir = getTaskOutputPath(job, name);

    return new MySQLRecordWriter(job, workDir, resultDir, name, progress);
  }

  public static class MySQLRecordWriter implements RecordWriter<NullWritable, MySQLRow> {
    private Progressable progress;
    private Path workDir;
    private Path outDir;
    private Path dataDir;
    private Path fifoPath;
    private Path sqlScriptPath;
    private Path pidPath;
    private Path socketPath;
    private JobConf conf;

    private Process mySqlProcess;

    private FileOutputStream fifoOutStream;

    private Schema schema;

    private static final String DB_NAME = "mrout";
    private static final String TABLE_NAME = "t";

    private static final int BUFFER_SIZE=256000;

    /** number of seconds to wait for mysql to create its pid and socket */
    private static final int MYSQL_START_TIMEOUT=3;

    MySQLRecordWriter(JobConf job,
                      Path workDir,
                      Path outDir,
                      String name,
                      Progressable progress)
      throws IOException
    {
      this.conf = job;
      this.workDir = workDir;
      this.outDir = outDir;
      this.progress = progress;
      this.dataDir = new Path(workDir, "mysqldata");
      this.fifoPath = new Path(workDir, "fifo");
      this.sqlScriptPath = new Path(workDir, "load.sql");
      this.pidPath = new Path(workDir, "mysql.pid");
      this.socketPath = new Path("/tmp/mysql.sock." + name);

      LOG.info("starting record writer. workdir: " + String.valueOf(workDir) +
               "   outDir: " + String.valueOf(outDir));
      installSqlDB();
      makeFifo();
    }

    private void setupDatabase(Schema schema) throws IOException {
      makeSqlScript(schema);
      startMySQL();

      fifoOutStream = new FileOutputStream(fifoPath.toString());
    }

    private void installSqlDB() throws IOException {
      LOG.info("installing mysql db");
      doCommand(new String[] {"mysql_install_db",
                              "--datadir=" + dataDir.toString()});
      LOG.info("mysql database initialized");
    }
    

    private void makeFifo() throws IOException {
      LOG.info("making fifo");
      doCommand(new String[] { "mkfifo",
                               fifoPath.toString() });
      LOG.info("fifo created");
    }

    private void makeSqlScript(Schema schema) throws IOException {
      FSDataOutputStream os = sqlScriptPath.getFileSystem(conf).create(sqlScriptPath);
      os.writeBytes("CREATE DATABASE `" + DB_NAME + "`;\n");
      os.writeBytes(schema.getCreateTableStatement(
                      "`" + DB_NAME + "`.`" + TABLE_NAME + "`",
                      "MyISAM").replace('\n', ' ') + "\n");
      os.writeBytes("LOAD DATA INFILE '" +
                    fifoPath.toString() + "' INTO TABLE " +
                    "`" + DB_NAME + "`.`" + TABLE_NAME + "` " +
                    "FIELDS TERMINATED BY '\001' " +
                    "LINES TERMINATED BY '\002';\n");
      os.close();
    }

    private void installShutdownHook() {
      Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            if (mySqlProcess == null)
              return;

            try {
              mySqlProcess.destroy();
              mySqlProcess.waitFor();
            } catch (Throwable t) {}
          }
        });
    }

    private void startMySQL() throws IOException {
      installShutdownHook();

      LOG.info("starting mysql server...");
      mySqlProcess = Runtime.getRuntime().exec(
        new String[] {
          "mysqld",
          "--datadir=" + dataDir.toString(),
          "--socket=" + socketPath.toString(),
          "--pid-file=" + pidPath.toString(),
          "--skip-networking",
          "--log-error=" + new Path(workDir, "mysql.err").toString(),
          "--init-file=" + sqlScriptPath.toString(),
          "--skip-innodb"
        });

      Path markerPath = new Path(new Path(dataDir, DB_NAME),
                                 TABLE_NAME + ".frm");

      LOG.info("Waiting for pid file and table to appear");

      FileSystem fs = workDir.getFileSystem(conf);

      boolean waiting = true;
      for (int i = MYSQL_START_TIMEOUT; i > 0 && waiting; i--) {
        waiting = false;
        if (!fs.exists(pidPath)) {
          LOG.info("PID file doesn't exist yet.");
          waiting = true;
        }

        if (!fs.exists(markerPath)) {
          LOG.info("markerPath doesn't exist yet.");
          waiting = true;
        }
        if (waiting) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }

        try {
          int exit = mySqlProcess.exitValue();
          String errString = readStreamFully(mySqlProcess.getErrorStream());
          throw new IOException("MySQL didnt start." +
                                "\nOutput:\n" + errString);
        } catch (IllegalThreadStateException itse) {
          // mysql proc still running
        }
      }

      if (waiting) {
        String errString = readStreamFully(mySqlProcess.getErrorStream());
        throw new IOException("timed out waiting on mysql start. stderr:\n" +
                              errString);
      } 

      LOG.info("MySQL has successfully started up. booya.com");
    }

    private void doCommand(String[] command) throws IOException {
      Process proc = Runtime.getRuntime().exec(command);
      while (true) {
        try {
          int status = proc.waitFor();
          if (status != 0) {
            String errString = readStreamFully(proc.getErrorStream());
            throw new IOException("non-zero exit status from " + command[0] +
              "\nOutput:\n" + errString);
          }
        } catch (InterruptedException ie) {
          continue;
        }
        return;
      }
    }

    private String readStreamFully(InputStream is) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      StringBuilder out = new StringBuilder();

      String line;
      do {
        if (! reader.ready()) break;
        line = reader.readLine();
        if (line != null) {
          out.append(line);
          out.append("\n");
        }
      } while (line != null);
      return out.toString();
    }

    public void write(NullWritable ignore, MySQLRow row) throws IOException {
      if (mySqlProcess == null) {
        LOG.info("first write! setting up database based on row schema");
        this.schema = row.getSchema();
        setupDatabase(row.getSchema());
      }

      LOG.info("write: " + String.valueOf(row));
      row.write(fifoOutStream, (byte)1);
      fifoOutStream.write((byte)2);
    }

    public void close(Reporter reporter) throws IOException {
      LOG.info("close");

      if (fifoOutStream != null) {
        fifoOutStream.close();
      }
    }
  }
}