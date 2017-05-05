package meepo.util.date;

import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by peiliping on 17-5-5.
 */
public class DateFormatter extends SimpleDateFormat {

    public DateFormatter(String pattern) {
        super("", Locale.US);
    }

    @Override public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition pos) {
        super.calendar.setTime(date);
        toAppendTo.append("'");
        toAppendTo.append(super.calendar.get(1));
        toAppendTo.append("-");
        if (super.calendar.get(2) < 9) {
            toAppendTo.append(0);
        }
        toAppendTo.append(super.calendar.get(2) + 1);
        toAppendTo.append("-");
        if (super.calendar.get(5) < 10) {
            toAppendTo.append(0);
        }
        toAppendTo.append(super.calendar.get(5));
        toAppendTo.append(" ");
        if (super.calendar.get(11) < 10) {
            toAppendTo.append(0);
        }
        toAppendTo.append(super.calendar.get(11));
        toAppendTo.append(":");
        if (super.calendar.get(12) < 10) {
            toAppendTo.append(0);
        }
        toAppendTo.append(super.calendar.get(12));
        toAppendTo.append(":");
        if (super.calendar.get(13) < 10) {
            toAppendTo.append(0);
        }
        toAppendTo.append(super.calendar.get(13));
        return toAppendTo;
    }
}
