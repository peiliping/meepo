package meepo.transform.report;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;

/**
 * Created by peiliping on 17-4-17.
 */
@Getter
public class MultiReport extends IReportItem {

    List<IReportItem> reports = Lists.newArrayList();

}
