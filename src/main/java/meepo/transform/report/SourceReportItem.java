package meepo.transform.report;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by peiliping on 17-3-13.
 */
@Setter @Getter @Builder public class SourceReportItem extends IReportItem {

    private String name;

    private long start;

    private long current;

    private long end;

    private boolean running;
}
