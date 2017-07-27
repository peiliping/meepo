package meepo.transform.source;

import meepo.transform.source.fake.SimpleNumSource;
import meepo.transform.source.kafka.Kafka010Source;
import meepo.transform.source.kafka.Kafka08Source;
import meepo.transform.source.parquet.ParquetFileSource;
import meepo.transform.source.rdb.*;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SourceType {

    SIMPLENUMSOURCE(SimpleNumSource.class),

    DBBYIDSOURCE(DBByIdSource.class),

    DBSYNCBYIDSOURCE(DBSyncByIdSource.class),

    DBSYNCBYTSSOURCE(DBSyncByTSSource.class),

    DBBYDATESOURCE(DBByDateSource.class),

    DBSYNCBYDATESOURCE(DBSyncByDateSource.class),

    PARQUETFILESOURCE(ParquetFileSource.class),

    KAFKA08SOURCE(Kafka08Source.class),

    KAFKA010SOURCE(Kafka010Source.class);

    public Class<? extends AbstractSource> clazz;

    SourceType(Class<? extends AbstractSource> clazz) {
        this.clazz = clazz;
    }
}
