package meepo.transform.report;

import lombok.Getter;

/**
 * Created by peiliping on 17-3-13.
 */
@Getter public abstract class IReportItem {

    private long timestamp = System.currentTimeMillis();

}
