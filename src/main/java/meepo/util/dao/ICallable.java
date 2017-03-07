package meepo.util.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by peiliping on 17-3-7.
 */
public abstract class ICallable<V> {

    public abstract void handleParams(PreparedStatement p) throws Exception;

    public abstract V handleResultSet(ResultSet r) throws Exception;

}
