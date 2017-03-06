package meepo.transform.source;

import meepo.transform.source.fake.SimpleNumSource;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SourceType {

    SIMPLENUMSOURCE(SimpleNumSource.class);


    public Class<? extends AbstractSource> clazz;

    SourceType(Class<? extends AbstractSource> clazz) {
        this.clazz = clazz;
    }
}
