//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package javaUtil;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateTimeUtil {
    private static final Logger logger = LoggerFactory.getLogger(DateTimeUtil.class);
    public static final int ONE_DAY_TIME = 86400000;

    public DateTimeUtil() {
    }

    public static int compare(Date date1, Date date2) {
        long compareResult = date1.getTime() - date2.getTime();
        return compareResult > 0L ? 1 : (compareResult == 0L ? 0 : -1);
    }

    public static Date getPrevDate() {
        return getPrevDate(DateTime.now().toDate());
    }

    public static Date getPrevDate(Date date) {
        if (null == date) {
            throw new IllegalArgumentException("参数date不能为null");
        } else {
            return (new DateTime(date)).minusDays(1).toDate();
        }
    }

    public static Date getNextDate() {
        return getNextDate(DateTime.now().toDate());
    }

    public static Date getNextDate(Date date) {
        if (null == date) {
            throw new IllegalArgumentException("参数date不能为null");
        } else {
            return (new DateTime(date)).plusDays(1).toDate();
        }
    }

    public static String getWeek(Date date) {
        return (new SimpleDateFormat("EEEE", Locale.CHINESE)).format(date);
    }

    public static Date dateStrToDate(String dateStr) {
        return DateTime.parse(dateStr).toDate();
    }

    public static Date longToDate(long time) {
        return (new DateTime(time)).toDate();
    }

    public static String longToDateStr(long time) {
        return longToDateStr(time, (String) null);
    }

    public static String longToDateStr(long time, String pattern) {
        return time == 0L ? "－－" : dateToStr(longToDate(time), pattern);
    }

    public static String dateToStr(Date date, String pattern) {
        return (new SimpleDateFormat(StringUtils.isNotBlank(pattern) ? pattern : "yyyy-MM-dd HH:mm:ss")).format(date);
    }

    public static String dateToStr(Date date) {
        return dateToStr(date, "yyyy-MM-dd HH:mm:ss");
    }

    public static long dateTimeStrToLong(String dateTimeStr, boolean isEndTime) {
        long result = dateTimeStrToLong(dateTimeStr);
        return isEndTime ? result + 999L : result;
    }

    public static long dateTimeStrToLong(String dateTimeStr) {
        return DateTime.parse(dateTimeStr, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).getMillis();
    }

    /**
     * string转long
     * @param dateTimeStr
     * @param formatStr
     * @return
     */
    public static long StrToLong(String dateTimeStr, String formatStr) {
        return DateTime.parse(dateTimeStr, DateTimeFormat.forPattern(formatStr)).getMillis();
    }

    public static long getStartTime(String startTime) {
        return dateTimeStrToLong(startTime + " 00:00:00");
    }

    public static long getEndTime(String endTime) {
        return dateTimeStrToLong(endTime + " 23:59:59", true);
    }

    public static Long getCurrentTime() {
        return Long.valueOf(DateTimeUtils.currentTimeMillis());
    }

    public static Date intToDate(int dateTime, String formatStr) {
        SimpleDateFormat simpledateformat = new SimpleDateFormat(formatStr);
        Date date = null;

        try {
            date = simpledateformat.parse(String.valueOf(dateTime));
        } catch (ParseException var5) {
            logger.error("转换8位int（20110101）的时间值为日期型出错", var5);
        }

        return date;
    }

    public static String getStringDate() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    public static String getStringDate2() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    public static String getStringTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }


    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }


    /**
     * 获取指定时间段
     * @param startTime
     * @param endTime
     * @return
     */
    public static List<String> getBetweenTime(String startTime, String endTime) {
        List<String> betweenTime = new ArrayList<String>();
        try {
            Date start = new SimpleDateFormat("yyyy-MM-dd").parse(startTime);
            Date end = new SimpleDateFormat("yyyy-MM-dd").parse(endTime);

            SimpleDateFormat outformat = new SimpleDateFormat("yyyy-MM-dd");

            Calendar sCalendar = Calendar.getInstance();
            sCalendar.setTime(start);
            int year = sCalendar.get(Calendar.YEAR);
            int month = sCalendar.get(Calendar.MONTH);
            int day = sCalendar.get(Calendar.DATE);
            sCalendar.set(year, month, day, 0, 0, 0);

            Calendar eCalendar = Calendar.getInstance();
            eCalendar.setTime(end);
            year = eCalendar.get(Calendar.YEAR);
            month = eCalendar.get(Calendar.MONTH);
            day = eCalendar.get(Calendar.DATE);
            eCalendar.set(year, month, day, 0, 0, 0);

            while (sCalendar.before(eCalendar)) {
                betweenTime.add(outformat.format(sCalendar.getTime()));
                sCalendar.add(Calendar.DAY_OF_YEAR, 1);
            }
            betweenTime.add(outformat.format(eCalendar.getTime()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return betweenTime;
    }


}
