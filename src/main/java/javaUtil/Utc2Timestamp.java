package javaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: lichuntao
 * @Function: （1）
 * （2）
 * ...
 */


public class Utc2Timestamp {
    public String utcTimeToLocalTime(String date) {
        if (date == null || date == "") {
            return null;
        }
        try {
            date = date.replace("Z", " UTC");
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z");
            Date dt = format.parse(date);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String format1 = df.format(dt);
            return format1;
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        String s = new Utc2Timestamp().utcTimeToLocalTime("2020-06-10T16:00:00.000Z");
        System.out.println(s);
    }
}
