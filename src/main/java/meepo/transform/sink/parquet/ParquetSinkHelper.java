package meepo.transform.sink.parquet;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetSinkHelper extends ParquetWriter<Object[]> {

    @Getter
    private boolean close = false;

    @SuppressWarnings("deprecation")
    public ParquetSinkHelper(Path file, MessageType schema) throws IOException {
        super(file, new ParquetSinkSupport(schema), CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE / 4, ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    @SuppressWarnings("deprecation")
    public ParquetSinkHelper(Path file, MessageType schema, Configuration conf) throws IOException {
        super(file, ParquetFileWriter.Mode.CREATE, new ParquetSinkSupport(schema), CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE / 8,
                ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION, conf);
    }

    @Override
    public void close() throws IOException {
        if (this.close) {
            return;
        }
        super.close();
        this.close = true;
    }
}
