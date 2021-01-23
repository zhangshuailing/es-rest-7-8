package nk.gk.wyl.elasticsearch.util.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class Util {

    /**
     * 获取唯一流水号
     * @return string
     */
    public static String getResourceId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    /**
     * title 获取当前时间
     * @return  时间
     */
    public static Date getDate() {
        return new Date();
    }

    /**
     * 获取字符串时间
     * @return 返回字符串时间
     */
    public static String getStrDate(){
        return DateFormatUtils.format(getDate(),"yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 校验字符串是否是数字.
     * @param str 字符串
     * @return
     */
    public static boolean checkNumber(String str){
        if(str != null && !"".equals(str.trim())){
            return str.matches("^[0-9]*$");
        }
        return false;
    }

    /**
     * 新增的添加公共的数据（create_time,create_by）
     * @param map 组合参数
     */
    public static void addMap(Map<String,Object> map,String username){
        if(!map.containsKey("create_time")){

            map.put("create_time",getStrDate());
        }
        map.put("create_user_id",username);
    }

    /**
     * 新增的添加公共的数据（update_time,update_by）
     * @param map 组合参数
     */
    public static void editMap(Map<String,Object> map,String username){
        if(!map.containsKey("update_time")){

            map.put("update_time",getStrDate());
        }
        map.put("update_user_id",username);
    }

    /**
     * 删除的添加公共的数据（del_time,del_by）
     * @param map 组合参数
     */
    public static void delMap(Map<String,Object> map,String username){
        map.put("del_time",getStrDate());
        map.put("del_by",username);
    }
}
