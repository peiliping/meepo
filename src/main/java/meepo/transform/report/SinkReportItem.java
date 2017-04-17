package meepo.transform.report;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by peiliping on 17-4-17.
 */
@Setter @Getter @Builder public class SinkReportItem extends IReportItem {

    private String name;

    private long count;

    private long batchCount;

    private long unsaturatedBatch;

    private boolean running;

}
