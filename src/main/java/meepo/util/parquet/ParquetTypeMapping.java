package meepo.util.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Created by peiliping on 17-3-9.
 */
public class ParquetTypeMapping {

    private static final Map<PrimitiveType.PrimitiveTypeName, Integer> P2J = Maps.newHashMap();
    private static final Map<Integer, PrimitiveType.PrimitiveTypeName> J2P = Maps.newHashMap();
    private static final Map<Integer, Integer> J2J = Maps.newHashMap();

    static {
        P2J.put(INT32, Types.INTEGER);
        P2J.put(INT64, Types.BIGINT);
        P2J.put(BOOLEAN, Types.BOOLEAN);
        P2J.put(FLOAT, Types.FLOAT);
        P2J.put(DOUBLE, Types.DOUBLE);
        P2J.put(BINARY, Types.VARCHAR);

        J2P.put(Types.TINYINT, INT32);
        J2P.put(Types.SMALLINT, INT32);
        J2P.put(Types.INTEGER, INT32);
        J2P.put(Types.BIGINT, INT64);
        J2P.put(Types.BIT, BOOLEAN);
        J2P.put(Types.BOOLEAN, BOOLEAN);
        J2P.put(Types.REAL, FLOAT);
        J2P.put(Types.FLOAT, FLOAT);
        J2P.put(Types.DOUBLE, DOUBLE);
        J2P.put(Types.VARCHAR, BINARY);
        J2P.put(Types.LONGVARCHAR, BINARY);

        J2J.put(Types.DATE, Types.BIGINT);
        J2J.put(Types.TIMESTAMP, Types.BIGINT);
    }

    // parquet file source
    public static List<Pair<String, Integer>> convert2JDBCTypes(MessageType schema) {
        List<Pair<String, Integer>> result = Lists.newArrayList();
        for (Type type : schema.getFields()) {
            Integer jdbcType = P2J.get(type.asPrimitiveType().getPrimitiveTypeName());
            Validate.notNull(jdbcType);
            result.add(Pair.of(type.getName(), jdbcType));
        }
        return result;
    }

    //parquet file sink
    public static List<Type> convert2Types(List<Pair<String, Integer>> schema) {
        final List<Type> types = Lists.newArrayList();
        schema.forEach(item -> {
            PrimitiveType.PrimitiveTypeName tmp = J2P.get(item.getRight());
            Validate.notNull(tmp);
            if (tmp == PrimitiveType.PrimitiveTypeName.BINARY) {
                types.add(new PrimitiveType(Type.Repetition.OPTIONAL, tmp, item.getLeft(), OriginalType.UTF8));
            } else {
                types.add(new PrimitiveType(Type.Repetition.OPTIONAL, tmp, item.getLeft()));
            }
        });
        return types;
    }

    //parquet type plugin
    public static void jdbcTypeMatchParquet(List<Pair<String, Integer>> sink) {
        for (int i = 0; i < sink.size(); i++) {
            Pair<String, Integer> item = sink.get(i);
            if (J2J.containsKey(item.getRight())) {
                Integer newType = J2J.get(item.getRight());
                Validate.notNull(J2P.get(newType));
                sink.set(i, Pair.of(item.getLeft(), newType));
            }
        }
    }

}
