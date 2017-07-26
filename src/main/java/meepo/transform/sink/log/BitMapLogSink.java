package meepo.transform.sink.log;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.hp.bit.Bit64Store;
import meepo.util.hp.bit.Roaring64BitMap;

/**
 * Created by peiliping on 17-7-13.
 */
public class BitMapLogSink extends AbstractSink {

    private String columnName;

    private int columnPosition;

    private Roaring64BitMap bitmap;

    private String keyName;

    public BitMapLogSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.columnName = context.get("columnName");
        this.columnPosition = context.getInteger("columnPosition", -1);
        this.keyName = context.getString("keyName");
    }

    @Override
    public void onStart() {
        super.onStart();
        if (this.columnPosition < 0) {
            if (this.columnName == null) {
                this.columnPosition = 0;
            } else {
                super.schema.forEach(x -> {
                    if (columnName.equals(x.getKey())) {
                        columnPosition = super.schema.indexOf(x);
                    }
                });
            }
        }
        this.bitmap = new Roaring64BitMap();
    }

    @Override
    public void timeOut() {
    }

    @Override
    public void event(DataEvent event) {
        Object x = event.getTarget()[this.columnPosition];
        if (x != null) {
            this.bitmap.add((Long) x);
            super.count++;
        }
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        this.bitmap.runOptimize();
        Bit64Store.getInstance().save(this.keyName, this.bitmap);
        this.bitmap = null;
    }
}
