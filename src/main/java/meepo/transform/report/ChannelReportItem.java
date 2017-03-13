package meepo.transform.report;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by peiliping on 17-3-13.
 */
@Setter @Getter @Builder public class ChannelReportItem extends IReportItem {

    private String name;

    private long capacity;

    private long remain;

}
