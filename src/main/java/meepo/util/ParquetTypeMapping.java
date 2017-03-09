package meepo.util;

import com.google.common.collect.Maps;
import org.apache.parquet.schema.PrimitiveType;

import java.sql.Types;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetTypeMapping {

    public static final Map<PrimitiveType.PrimitiveTypeName, Integer> P2J = Maps.newHashMap();

    static {
        P2J.put(INT32, Types.INTEGER);
        P2J.put(INT64, Types.BIGINT);
        P2J.put(BOOLEAN, Types.BOOLEAN);
        P2J.put(FLOAT, Types.FLOAT);
        P2J.put(DOUBLE, Types.DOUBLE);
        P2J.put(BINARY, Types.VARCHAR);
    }

}
