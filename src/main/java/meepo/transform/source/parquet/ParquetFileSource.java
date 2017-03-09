package meepo.transform.source.parquet;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.source.AbstractSource;
import meepo.util.ParquetTypeMapping;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetFileSource extends AbstractSource {

    private ParquetReader<Group>[] readers;

    private int columnsNum;

    private MessageType msgType;

    public ParquetFileSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        String inputDir = context.get("inputdir");
        Validate.notNull(inputDir);
        File dir = Paths.get(inputDir).toFile();
        Validate.isTrue(dir.isDirectory());
        List<String> fileNames = Lists.newArrayList(dir.list((dir1, name1) -> name1.endsWith(".parquet") || name1.endsWith(".pqt")));
        Validate.isTrue(fileNames.size() > 0);
        Collections.sort(fileNames);
        this.readers = new ParquetReader[fileNames.size()];

        try {
            Path filePath = new Path(inputDir + fileNames.get(0));
            ParquetMetadata metaData = ParquetFileReader.readFooter(new Configuration(), filePath);
            this.msgType = metaData.getFileMetaData().getSchema();
            this.columnsNum = this.msgType.getFieldCount();
            GroupReadSupport grs = new GroupReadSupport();
            grs.init(new Configuration(), null, this.msgType);
            for (Type type : this.msgType.getFields()) {
                Integer jdbcType = ParquetTypeMapping.P2J.get(type.asPrimitiveType().getPrimitiveTypeName());
                Validate.notNull(jdbcType);
                super.schema.add(Pair.of(type.getName(), jdbcType));
            }
            for (int i = 0; i < fileNames.size(); i++) {
                this.readers[i] = ParquetReader.builder(grs, new Path(inputDir + fileNames.get(i))).build();
            }
        } catch (Exception e) {
            LOG.error("Init Parquet Error :", e);
            Validate.isTrue(false);
        }
    }

    @Override public void eventFactory(DataEvent de) {
        de.setSource(new Object[this.columnsNum]);
    }

    private Group record = null;

    private int fileIndex = 0;

    @Override public void work() {
        if (this.fileIndex >= this.readers.length) {
            super.RUNNING = false;
            return;
        }
        try {
            this.record = this.readers[this.fileIndex].read();
            if (this.record == null) {
                this.readers[this.fileIndex].close();
                this.fileIndex++;
                return;
            }
            DataEvent de = feedOne();
            for (int i = 0; i < this.columnsNum; i++) {
                if (this.record.getFieldRepetitionCount(i) == 0) {
                    de.getSource()[i] = null;
                    continue;
                }
                switch (this.msgType.getFields().get(i).asPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        de.getSource()[i] = this.record.getInteger(i, 0);
                        break;
                    case INT64:
                        de.getSource()[i] = this.record.getLong(i, 0);
                        break;
                    case BOOLEAN:
                        de.getSource()[i] = this.record.getBoolean(i, 0);
                        break;
                    case BINARY:
                        de.getSource()[i] = this.record.getBinary(i, 0).toStringUsingUTF8();
                        break;
                    case FLOAT:
                        de.getSource()[i] = this.record.getFloat(i, 0);
                        break;
                    case DOUBLE:
                        de.getSource()[i] = this.record.getDouble(i, 0);
                        break;
                    default:
                        throw new ParquetEncodingException("Unsupported column type: " + this.schema.get(i));
                }
            }
            super.pushOne();
        } catch (Throwable e) {
            LOG.error("Handle Parquet Data Error", e);
        }
    }
}
