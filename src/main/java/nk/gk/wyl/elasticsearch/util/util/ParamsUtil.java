package nk.gk.wyl.elasticsearch.util.util;

import com.alibaba.fastjson.JSON;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: 根据查询条件生成
 * @Author: zhangshuailing
 * @CreateDate: 2020/8/29 0:09
 * @UpdateUser: zhangshuailing
 * @UpdateDate: 2020/8/29 0:09
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class ParamsUtil {
    /**
     * 查询条件中抽取出来 模糊查询 精确查找 排序
     *
     * @param map          查询条件
     * @param search       模糊查询
     * @param exact_search 精确查找
     * @param in_search    in 查询
     * @param order        排序
     */
    public static void searchMap(Map<String, Object> map,
                                 Map<String, String> search,
                                 Map<String, String> exact_search,
                                 Map<String, String> order,
                                 Map<String, List<String>> in_search) throws Exception {
        // 模糊查询
        search = getMap(map, "search");
        // 精确查找
        exact_search = getMap(map, "exact_search");
        // 排序
        order = getMap(map, "order");
        // in 字句
        in_search = getList(map, "in_search");
    }

    /**
     * 转map
     *
     * @param map   参数
     * @param field 参数中的key
     * @return
     * @throws Exception
     */
    public static Map<String, String> getMap(Map<String, Object> map, String field) throws Exception {
        Map<String, String> query = null;
        if (map.containsKey(field)) {
            try {
                query = (Map<String, String>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                try{
                    query = (Map<String,String>)(JSON.parse(map.get(field).toString()));
                }catch (Exception e1){
                    throw new Exception("参数 " + field + " 类型错误");
                }
            }
        }
        return query==null?new HashMap<>():query;
    }

    /**
     * 获取和校验 Map<String,Map<String,String>> 参数
     * @param map 参数
     * @param field 字段
     * @return 返回 Map<String,Map<String,String>> rang_search
     */
    public static Map<String,Map<String,String>> getMapParamsRangSearch(Map<String, Object> map, String field) throws Exception{
        Map<String, Map<String,String>> query = null;
        if (map.containsKey(field)) {
            try {
                query = (Map<String, Map<String, String>>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                try{
                    query = (Map<String, Map<String, String>>) JSON.parse(map.get(field).toString());
                }catch (Exception e1){
                    throw new Exception("参数 " + field + " 类型错误");
                }
            }
        }
        return query==null?new HashMap<>():query;
    }


    /**
     * 转map
     *
     * @param map   参数
     * @param field 参数中的key
     * @return
     * @throws Exception
     */
    public static Map<String, Object> getObj(Map<String, Object> map, String field) throws Exception {
        Map<String, Object> query = null;
        if (map.containsKey(field)) {
            try {
                query = (Map<String, Object>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                try{
                    query = (Map<String,Object>)(JSON.parse(map.get(field).toString()));
                }catch (Exception e1){
                    throw new Exception("参数 " + field + " 类型错误");
                }
            }
        }
        if(query.isEmpty()){
            throw new Exception("参数 " + field + " 不能为空");
        }
        return query==null?new HashMap<>():query;
    }

    /**
     * 转map
     *
     * @param map   参数
     * @param field 参数中的key
     * @return
     * @throws Exception
     */
    public static Map<String, Integer> getMapInteger(Map<String, Object> map, String field) throws Exception {
        Map<String, Integer> query = null;
        if (map.containsKey(field)) {
            try {
                query = (Map<String, Integer>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                try{
                    query = (Map<String,Integer>)(JSON.parse(map.get(field).toString()));
                }catch (Exception e1){
                    throw new Exception("参数 " + field + " 类型错误");
                }
            }
        }
        return query==null?new HashMap<>():query;
    }
    /**
     * 获取参数中的数组
     *
     * @param map
     * @param field
     * @return
     */
    public static String[] getMapArray(Map<String, Object> map, String field) throws Exception {
        List<String> list = getArrayList(map,field);
        String[] array = listToArray(list);
        return array;
    }

    /**
     * 校验数组
     * @param map 参数
     * @param field 字段
     * @return 返回数据
     * @throws Exception 异常信息
     */
    public static String[] checkMapArray(Map<String, Object> map, String field) throws Exception{
        if(StringUtils.isEmpty(map.get(field))){
            throw new Exception("参数 【" + field + "】不能为空");
        }
        String[] result  = getMapArray(map,field);
        if(result==null || result.length == 0){
            throw new Exception("参数 【" + field + "】不能为空");
        }
        return result;
    }

    /**
     * 获取参数中的集合
     *
     * @param map
     * @param field
     * @return
     */
    public static List<String> getArrayList(Map<String, Object> map, String field) throws Exception {
        List<String> array = null;
        if (!StringUtils.isEmpty(map.get(field))) {
            try {
                array = (List<String>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                try{
                    array = (List<String>) JSON.parse(map.get(field).toString());
                }catch (Exception e1){
                    throw new Exception("参数 " + field + " 类型错误");
                }
            }
        }
        return array==null?new ArrayList<>():array;
    }
    /**
     * 校验参数中的集合
     *
     * @param map
     * @param field
     * @return
     */
    public static List<String> checkArrayList(Map<String, Object> map, String field) throws Exception {
        List<String> array = getArrayList(map,field);
        if(array==null || array.size()==0){
            throw new Exception("参数 " + field + " 不能为空");
        }
        return array;
    }

    /**
     * 获取参数中的数组
     *
     * @param map
     * @param field
     * @return
     */
    public static List<String> getMapList(Map<String, Object> map, String field) throws Exception {
        List<String> array = null;
        if (!StringUtils.isEmpty(map.get(field))) {
            try {
                array = (List<String>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                throw new Exception(field + " 参数类型错误！");
            }
        }
        return array==null?new ArrayList<>():array;
    }

    /**
     * 获取参数中的数组
     *
     * @param map
     * @param field
     * @return
     */
    public static List<String> checkMapList(Map<String, Object> map, String field) throws Exception {
        List<String> array = null;
        if (!StringUtils.isEmpty(map.get(field))) {
            try {
                array = (List<String>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                throw new Exception(field + " 参数类型错误！");
            }
        }else{
            throw new Exception(field + " 参数不能为空！");
        }
        return array==null?new ArrayList<>():array;
    }


    /**
     * 转map
     *
     * @param map   参数
     * @param field 参数中的key
     * @return
     * @throws Exception
     */
    public static Map<String, List<String>> getList(Map<String, Object> map, String field) throws Exception {
        Map<String, List<String>> query = null;
        if (map.containsKey(field)) {
            try {
                query = (Map<String, List<String>>) JSON.toJSON(map.get(field));
            } catch (Exception e) {
                throw new Exception("参数 " + field + " 类型错误");
            }
        }
        return query==null?new HashMap<>():query;
    }

    /**
     * 校验map 参数
     *
     * @param map
     * @param name
     * @return
     * @throws Exception
     */
    public static Map<String, Object> checkMap(Map<String, Object> map, String name) throws Exception {
        if (StringUtils.isEmpty(map.get(name))) {
            throw new Exception("缺少 " + name + " 参数");
        }
        Map<String, Object> result = getObj(map,name);
        return result;
    }

    /**
     * 获取参数值
     *
     * @param map
     * @param name
     * @return
     * @throws Exception
     */
    public static String getStrValue(Map<String, String> map, String name) {
        if (StringUtils.isEmpty(map.get(name))) {
            return "";
        }
        return map.get(name).toString();
    }

    /**
     * 获取参数值
     *
     * @param map
     * @param name
     * @return
     * @throws Exception
     */
    public static String getValue(Map<String, Object> map, String name) {
        if (StringUtils.isEmpty(map.get(name))) {
            return "";
        }
        return map.get(name).toString();
    }

    /**
     * 校验参数值
     *
     * @param map
     * @param name
     * @return
     * @throws Exception
     */
    public static String checkValue(Map<String, Object> map, String name) throws Exception {
        if (StringUtils.isEmpty(map.get(name))) {
            throw new Exception("缺少 " + name + " 参数");
        }
        return map.get(name).toString();
    }

    /**
     * 获取参数值
     *
     * @param map
     * @param name
     * @return
     * @throws Exception
     */
    public static boolean getBlValue(Map<String, Object> map, String name) {
        if (StringUtils.isEmpty(map.get(name))) {
            return false;
        }
        try{
            return Boolean.parseBoolean (map.get(name).toString()) ;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 获取用户的uid
     *
     * @param map
     * @return
     */
    public static String getUid(Map<String, Object> map) {
        return getValue(map, "uid");
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
     * 获取字符串转int
     * @param field 字段
     * @param str 字符串
     * @return 返回int
     * @throws Exception 异常信息
     */
    public static int getNumber(String field,String str) throws Exception{
        boolean bl = checkNumber(str);
        if(bl){
            return Integer.parseInt(str);
        }else{
            throw new Exception("参数 "+ field + " 格式错误");
        }
    }

    /**
     *
     * @param params
     * @param name
     * @param default_size
     * @return
     * @throws Exception
     */
    public static int getNumberParams(Map params,String name,int default_size) throws Exception{
        String size = ParamsUtil.getValue(params,name);
        boolean bl = ParamsUtil.checkNumber(size);
        int size_num = default_size;
        if(bl){
            size_num  = Integer.parseInt(size);
        }
        return size_num;
    }

    /**
     * list 转 数组
     * @param list
     * @return
     */
    public static String[] listToArray(List<String> list){
        if(list==null || list.size()==0){
            return new String[]{};
        }
        String[] array = list.toArray(new String[0]);
        return array;
    }

    /**
     * 获取集合数据
     * @param map
     * @param key
     * @return
     * @throws Exception
     */
    public static List<Map<String,Object>> checkListMapValue(Map<String,Object> map,String key) throws Exception{
        List<Map<String,Object>> query = null;
        if (map.containsKey(key)) {
            try {
                query = (List<Map<String, Object>>) JSON.toJSON(map.get(key));
            } catch (Exception e) {
                throw new Exception("参数 " + key + " 类型错误");
            }
        }
        return query==null?new ArrayList<>():query;
    }
}
