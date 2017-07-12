package meepo.transform.channel.plugin.custom;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import meepo.transform.report.PluginReport;

/**
 * Created by peiliping on 17-4-17.
 */
@Getter
@Setter
@Builder
class ConditionReplacePluginReport extends PluginReport {

    private String name;

    private long matchCondition;
}
