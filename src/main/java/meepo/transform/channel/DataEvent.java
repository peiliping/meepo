package meepo.transform.channel;

import com.lmax.disruptor.EventFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by peiliping on 17-3-2.
 */
@Setter
@Getter
@ToString
public class DataEvent {

    protected transient Object[] source;

    protected Object[] target;

    private boolean valid = true;

    public static EventFactory<DataEvent> INT_ENEVT_FACTORY = () -> new DataEvent();

}
