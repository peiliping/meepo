package meepo.transform.source;

import meepo.transform.source.fake.SimpleNumSource;
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

    PARQUETFILESOURCE(ParquetFileSource.class);

    public Class<? extends AbstractSource> clazz;

    SourceType(Class<? extends AbstractSource> clazz) {
        this.clazz = clazz;
    }
}
