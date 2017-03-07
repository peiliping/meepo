package meepo.util.dao;

import java.sql.PreparedStatement;

/**
 * Created by peiliping on 17-3-7.
 */
public abstract class ResultSetICallable<V> extends ICallable<V> {

    @Override public void handleParams(PreparedStatement p) throws Exception {
    }

}
