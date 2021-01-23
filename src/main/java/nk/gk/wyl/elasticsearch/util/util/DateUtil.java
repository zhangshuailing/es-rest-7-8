package nk.gk.wyl.elasticsearch.util.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
* @Description:    时间组件
* @Author:         zhangshuailing
* @CreateDate:     2019/11/11 14:37
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2019/11/11 14:37
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
public class DateUtil {
    /**
     * date2比date1多的天数
     * @param date1
     * @param date2
     * @return
     */
    public static int differentDays(Date date1, Date date2) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        int day1= cal1.get(Calendar.DAY_OF_YEAR);
        int day2 = cal2.get(Calendar.DAY_OF_YEAR);
        int year1 = cal1.get(Calendar.YEAR);
        int year2 = cal2.get(Calendar.YEAR);
        if(year1 != year2){ //同一年
            int timeDistance = 0 ;
            for(int i = year1 ; i < year2 ; i ++) {
                if(i%4==0 && i%100!=0 || i%400==0) {  //闰年
                    timeDistance += 366;
                } else { //不是闰年
                    timeDistance += 365;
                }
            }
            return timeDistance + Math.abs(day2-day1) ;
        } else { //不同年
            System.out.println("判断day2 - day1 : " + (day2-day1));
            return  Math.abs(day2-day1);
        }
    }

   /* public static void main(String[] args) {
        int total = 1002;
        System.out.println((int)total/(5*10));
    }*/

    /**
     * 将字符串时间转成时间搓
     * @param time 字符串时间
     * @param format 转化格式
     * @return
     */
    public static long getTimestamp(String time,String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        long ts = 0;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            ts = 0;
        }
        ts = date.getTime();
        return ts;
    }

    /**
     * 将字符串时间转成时间搓
     * @param time 字符串时间
     * @param format 转化格式
     * @return
     */
    public static Date getDate(String time,String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            //ts = 0;
        }
        return date;
    }
    /**
     * 将字符串时间转成时间搓
     * @param time 字符串时间
     * @param format 转化格式
     * @return
     */
    public static String getDateStr(String time,String format) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        String str_date = "";
        try {
            date = simpleDateFormat.parse(time);
            str_date = DateFormatUtils.format(date,format);
        } catch (ParseException e) {
            throw new Exception("时间类型格式错误");
        }
        return str_date;
    }

    /**
     * 将字符串时间转成时间搓
     * @param field 字段
     * @param time 字符串时间
     * @param format 转化格式
     * @return
     */
    public static String checkDateStr(String field,String time,String format) throws Exception{
        try{
            String str_date = getDateStr(time,format);
            return str_date;
        }catch (Exception e){
            throw new Exception("参数 "+field+" 类型格式错误");
        }
    }

    public static void main(String[] args) {
        String year = "2020-01";
        String format = "yyyy-MM";
        try {
           String d1 = joinDateLastDay(year,format);
            System.out.println(d1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据传入的年 月  类型 来获取 第一天
     * @param time 时间字符串
     * @param format 格式
     * @return
     */
    public static String joinDateFirstDay(String time,String format) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            throw new Exception("时间类型格式错误");
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH)+1;
        return getFirstDayOfMonth1(year,month);
    }
    /**
     * 根据传入的年 月  类型 来获取 最后一天
     * @param time 时间字符串
     * @param format 格式
     * @return
     */
    public static String joinDateLastDay(String time,String format) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            throw new Exception("时间类型格式错误");
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH)+1;
        return getLastDayOfMonth1(year,month);
    }

    /**
     * 获取指定当前日期往前推的天数的日期
     * @param time
     * @return
     */
    public static String getDate(String time,int number){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        Date date=null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        calendar.setTime(date);
        int day=calendar.get(Calendar.DATE);
        // 后一天为 +1   前一天 为-1
        calendar.set(Calendar.DATE,day-number);
        String lastDay = sdf.format(calendar.getTime());
        return lastDay;
    }

    /**
     * 判断两个字符串时间差（天数）
     * @param date1 默认是字符串 年月日 时分秒
     * @param date2 默认是字符串 年月日 时分秒
     * @return
     */
    public static int differentDays(String date1, String date2) {
        Date date_1 = parseDate(date1);
        Date date_2 = parseDate(date2);
        if(date1 != null && date_2 !=null){
            int day = differentDays(date_1,date_2);
            return day;
        }
        return -1;
    }

    /**
     * 格式
     */
    private static String[] parsePatterns = {"yyyy-MM-dd","yyyy年MM月dd日",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy/MM/dd",
            "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", "yyyyMMdd"};

    /**
     * 判断是否是字符串日期
     * @param string
     * @return
     */
    public static Date parseDate(String string) {
        if (string == null) {
            return null;
        }
        try {
            return DateUtils.parseDate(string, parsePatterns);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 获取指定年月的第一天
     * @param year
     * @param month
     * @return
     */
    public static String getFirstDayOfMonth1(int year, int month) {
        Calendar cal = Calendar.getInstance();
        //设置年份
        cal.set(Calendar.YEAR, year);
        //设置月份
        cal.set(Calendar.MONTH, month-1);
        //获取某月最小天数
        int firstDay = cal.getMinimum(Calendar.DATE);
        //设置日历中月份的最小天数
        cal.set(Calendar.DAY_OF_MONTH,firstDay);
        //格式化日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(cal.getTime());
    }

    /**
     * 获取指定年月的最后一天
     * @param year
     * @param month
     * @return
     */
    public static String getLastDayOfMonth1(int year, int month) {
        Calendar cal = Calendar.getInstance();
        //设置年份
        cal.set(Calendar.YEAR, year);
        //设置月份
        cal.set(Calendar.MONTH, month-1);
        //获取某月最大天数
        int lastDay = cal.getActualMaximum(Calendar.DATE);
        //设置日历中月份的最大天数
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        //格式化日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(cal.getTime());
    }
}
