package meepo.transform.channel.plugin.custom;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.plugin.DefaultPlugin;
import meepo.transform.config.TaskContext;
import meepo.transform.report.IReportItem;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by peiliping on 17-7-11.
 */
public class ConditionReplacePlugin extends DefaultPlugin {

    private String conditionColName;

    private int conditionColPos;

    private String conditionVal;

    private String sourceColName;

    private int sourceColPos;

    private String targetColName;

    private int targetColPos;

    private AtomicLong metricMatchCondition = new AtomicLong(0);

    public ConditionReplacePlugin(TaskContext context) {
        super(context);
        this.conditionColName = context.getString("conditionColName");
        this.conditionColPos = context.getInteger("conditionColPos", -1);
        this.conditionVal = context.getString("conditionVal");
        this.sourceColName = context.getString("sourceColName");
        this.sourceColPos = context.getInteger("sourceColPos", -1);
        this.targetColName = context.getString("targetColName");
        this.targetColPos = context.getInteger("targetColPos", -1);
    }

    @Override
    public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        super.autoMatchSchema(source, sink);

        if (this.conditionColPos < 0) {
            source.forEach(i -> {
                if (this.conditionColName.equals(i.getLeft())) {
                    this.conditionColPos = source.indexOf(i);
                }
            });
        }

        if (this.sourceColPos < 0) {
            source.forEach(i -> {
                if (this.sourceColName.equals(i.getLeft())) {
                    this.sourceColPos = source.indexOf(i);
                }
            });
        }

        if (this.targetColPos < 0) {
            source.forEach(i -> {
                if (this.targetColName.equals(i.getLeft())) {
                    this.targetColPos = source.indexOf(i);
                }
            });
        }
    }

    @Override
    public void convert(DataEvent de, boolean theEnd) {
        if (this.conditionVal.equals(de.getSource()[this.conditionColPos])) {
            String s = (String) de.getSource()[this.sourceColPos];
            String[] sarray = s.split("/");
            if (sarray.length >= 2) {
                String r = sarray[0] + "/" + sarray[1];
                de.getSource()[targetColPos] = r;
                this.metricMatchCondition.incrementAndGet();
            }
        }
        super.convert(de, theEnd);
    }

    @Override
    public IReportItem report() {
        return ConditionReplacePluginReport.builder().name(this.getClass().getSimpleName()).matchCondition(this.metricMatchCondition.get()).build();
    }
}
