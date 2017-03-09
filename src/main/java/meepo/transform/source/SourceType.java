package meepo.transform.source;

import meepo.transform.source.fake.SimpleNumSource;
import meepo.transform.source.parquet.ParquetFileSource;
import meepo.transform.source.rdb.DBSource;
import meepo.transform.source.rdb.DBSyncSource;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SourceType {

    SIMPLENUMSOURCE(SimpleNumSource.class),

    DBSOURCE(DBSource.class),

    DBSYNCSOURCE(DBSyncSource.class),

    PARQUETFILESOURCE(ParquetFileSource.class);


    public Class<? extends AbstractSource> clazz;

    SourceType(Class<? extends AbstractSource> clazz) {
        this.clazz = clazz;
    }
}
