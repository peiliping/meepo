package meepo.util.dao;

import java.sql.ResultSet;

/**
 * Created by peiliping on 17-3-7.
 */
public abstract class ParamsICallable<V> extends ICallable<V> {

    @Override public V handleResultSet(ResultSet r) throws Exception {
        return null;
    }

}
