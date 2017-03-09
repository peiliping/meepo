package meepo.transform.sink.parquet;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.ParquetTypeMapping;
import meepo.util.Util;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetSink extends AbstractSink {

    private String tableName;

    private String outputDir;

    private long rollingSize;

    private long counter;

    private String hdfsConfDir;

    private int part = 0;

    private ParquetSinkHelper sinkHelper;

    private List<Type> types;

    public ParquetSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.tableName = context.getString("tableName");
        this.outputDir = context.get("outputdir");
        this.rollingSize = context.getLong("rollingsize", Long.MAX_VALUE);
        this.hdfsConfDir = context.get("hdfsconfdir");
    }

    private void initSinkHelper() {
        if (this.sinkHelper != null) {
            return;
        }
        try {
            String fileName = this.outputDir + this.tableName + "-" + this.indexOfSinks + "-" + this.part + "-" + System.currentTimeMillis() / 1000 + ".parquet";
            if (this.hdfsConfDir == null) {
                this.sinkHelper = new ParquetSinkHelper(new Path(fileName), new MessageType(this.tableName, this.types));
            } else {
                this.sinkHelper = new ParquetSinkHelper(new Path(fileName), new MessageType(this.tableName, this.types), this.hdfsConfDir);
            }
        } catch (Exception e) {
            LOG.error("Init Parquet File Error :", e);
        }
    }

    @Override public void onStart() {
        super.onStart();
        this.types = ParquetTypeMapping.convert2Types(super.schema);
        initSinkHelper();
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

    private void closeParquet() {
        if (this.sinkHelper != null) {
            try {
                this.sinkHelper.close();
                this.sinkHelper = null;
                this.counter = 0;
                this.part++;
            } catch (IOException e) {
                LOG.error("Close Parquet File Error :", e);
            }
        }
    }

    @Override public void onShutdown() {
        this.closeParquet();
        super.onShutdown();
    }

    @Override public void timeOut() {
    }
}
