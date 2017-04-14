package meepo.transform.sink.parquet;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.ParquetTypeMapping;
import meepo.util.Util;
import org.apache.avro.Schema;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetSink extends AbstractSink {

    private String tableName;

    private MessageType messageType;

    private String outputDir;

    private long rollingSize;

    private long counter;

    private int part = 0;

    private String hdfsConfDir;

    private ParquetSinkHelper sinkHelper;

    public ParquetSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.tableName = context.getString("tableName");
        this.outputDir = context.get("outputdir");
        this.rollingSize = context.getLong("rollingsize", Long.MAX_VALUE);
        this.hdfsConfDir = context.get("hdfsconfdir");
    }

    @Override public void onStart() {
        super.onStart();
        this.messageType = new MessageType(this.tableName, ParquetTypeMapping.convert2Types(super.schema));
        initHDFS();
    }

    @Override public void onEvent(Object event) throws Exception {
        initSinkHelper();
        try {
            this.sinkHelper.write(((DataEvent) event).getTarget());
            if (++this.counter >= this.rollingSize) {
                this.closeParquet();
            }
        } catch (Exception e) {
            LOG.error("ParquetFilesWriter Write Data Error :", e);
            Util.sleep(3);
        }
    }

    @Override public void onShutdown() {
        this.closeParquet();
        super.onShutdown();
    }

    @Override public void timeOut() {
    }

    public void initHDFS() {
        if (this.hdfsConfDir == null || super.indexOfSinks > 0) {
            return;
        }
        try {
            FileSystem hdfs = FileSystem.get(createConf(this.hdfsConfDir));
            Path metap = new Path(this.outputDir + "/.metadata");
            if (!hdfs.exists(metap)) {
                hdfs.mkdirs(metap);
            }
            Path avscp = new Path(this.outputDir + "/.metadata/schema.avsc");
            if (!hdfs.exists(avscp)) {
                FSDataOutputStream fsd = hdfs.create(avscp);
                Schema avsc = (new AvroSchemaConverter()).convert(this.messageType);
                fsd.writeChars(avsc.toString());
                fsd.flush();
                fsd.close();
            }
        } catch (Exception e) {
            LOG.error("Init HDFS Error :", e);
            Validate.isTrue(false);
        }
    }

    private void initSinkHelper() {
        if (this.sinkHelper != null) {
            return;
        }
        try {
            String fileName = this.outputDir + "/" + this.tableName + "-" + super.indexOfSinks + "-" + this.part + "-" + System.currentTimeMillis() / 1000 + ".parquet";
            this.sinkHelper = (this.hdfsConfDir == null ?
                    new ParquetSinkHelper(new Path(fileName), this.messageType) :
                    new ParquetSinkHelper(new Path(fileName), this.messageType, createConf(this.hdfsConfDir)));
        } catch (Exception e) {
            LOG.error("Init Parquet File Error :", e);
            Validate.isTrue(false);
        }
    }

    private void closeParquet() {
        if (this.sinkHelper == null) {
            return;
        }
        try {
            this.sinkHelper.close();
            this.sinkHelper = null;
            this.counter = 0;
            this.part++;
        } catch (IOException e) {
            LOG.error("Close Parquet File Error :", e);
        }
    }

    public static Configuration createConf(String classpath) {
        Configuration conf = new Configuration();
        conf.addResource(new Path(classpath + "core-site.xml"));
        conf.addResource(new Path(classpath + "hdfs-site.xml"));
        return conf;
    }
}
