package meepo.transform.channel.plugin.TypeConvert;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import meepo.transform.channel.DataEvent;
import org.apache.commons.lang3.Validate;

import java.sql.Types;
import java.util.Date;
import java.util.Map;

/**
 * Created by peiliping on 17-3-8.
 */
@Getter @Setter @Builder public class NotMatchItem {

    private final static Map<String, Handler> HANDLERS = Maps.newHashMap();

    private final static String SPLIT = "->";

    static {
        HANDLERS.put(Types.INTEGER + SPLIT + Types.BIGINT, o -> Long.valueOf((Integer) o));

        HANDLERS.put(Types.BIGINT + SPLIT + Types.TIMESTAMP, o -> new Date((Long) o));
        HANDLERS.put(Types.BIGINT + SPLIT + Types.DATE, o -> new Date((Long) o));

        HANDLERS.put(Types.TIMESTAMP + SPLIT + Types.BIGINT, o -> ((Date) o).getTime());

        HANDLERS.put(Types.DATE + SPLIT + Types.BIGINT, o -> ((Date) o).getTime());
    }

    private int columnIndex;

    private String sourceFieldName;

    private int sourceFieldType;

    private String targetFieldName;

    private int targetFieldType;

    private Handler handler;

    public NotMatchItem init() {
        this.handler = HANDLERS.get(this.sourceFieldType + SPLIT + this.targetFieldType);
        Validate.notNull(this.handler);
        return this;
    }

    public void convert(DataEvent de) {
        de.getSource()[this.columnIndex] = de.getSource()[this.columnIndex] == null ? null : this.handler.handle(de.getSource()[this.columnIndex]);
    }

    interface Handler {
        Object handle(Object o);
    }
}
