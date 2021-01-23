package nk.gk.wyl.elasticsearch.util.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import nk.gk.wyl.elasticsearch.data.EsDataUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 生成 BoolQueryBuilder  查询
 */
public class QueryUtil {
    // 定义删除的标志位
    private static String _deleted="_deleted";
    // 定义是否开始逻辑删除
    private static boolean is_logic = false;

    public static String get_deleted() {
        return _deleted;
    }

    public static void set_deleted(String _deleted) {
        QueryUtil._deleted = _deleted;
    }

    public static boolean isIs_logic() {
        return is_logic;
    }

    public static void setIs_logic(boolean is_logic) {
        QueryUtil.is_logic = is_logic;
    }

    /**
     * 获取初始化的BoolQueryBuilder
     * @return 返回 BoolQueryBuilder
     */
    public static BoolQueryBuilder getCommonBoolQueryBuilder(){
        // 初始化布尔查询【boolQueryBuilder】
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(isIs_logic()){
            // 1 删除
            boolQueryBuilder.mustNot(QueryBuilders.termQuery(get_deleted(),"1"));
        }
        return boolQueryBuilder;
    }

    /**
     * 根据数据编号生成 BoolQueryBuilder
     * @param id 数据编号
     * @return 返回 BoolQueryBuilder
     */
    public static BoolQueryBuilder getBoolQueryBuilderById(String id){
        // 初始化的BoolQueryBuilder
        BoolQueryBuilder boolQueryBuilder = getCommonBoolQueryBuilder();
        // -id
        boolQueryBuilder.must(QueryBuilders.termQuery("_id",id));
        return boolQueryBuilder;
    }


    /**
     * 根据查询条件生成es查询条件
     * @param client
     * @param index 索引名称
     * @param params 参数
     * @return 返回 BoolQueryBuilder
     * @throws Exception 异常信息
     */
    public static BoolQueryBuilder getBoolQueryBuilder(RestHighLevelClient client,
                                                       String index,
                                                       Map<String,Object> params) throws Exception{
        // 定义布尔条件
        BoolQueryBuilder boolQueryBuilder  = getCommonBoolQueryBuilder();
        // 精确查询
        Map<String,String>  exact_search = ParamsUtil.getMap(params, "exact_search");
        // 精确查询生成布尔条件
        boolQueryBuilder = getBoolQueryBuilderExact(client,index,boolQueryBuilder,exact_search);
        // 模糊查询
        Map<String,String> search = ParamsUtil.getMap(params, "search");
        // 模糊查询生成布尔条件
        boolQueryBuilder = getBoolQueryBuilderLike(client,index,boolQueryBuilder,search);
        // in 字句
        Map<String, List<String>> in_search = ParamsUtil.getList(params, "in_search");
        // in查询生成布尔条件
        boolQueryBuilder = getBoolQueryBuilderIn(boolQueryBuilder,in_search);
        // 获取 rang_search 参数
        Map<String,Map<String,String>> rang_search = ParamsUtil.getMapParamsRangSearch(params,"rang_search");
        boolQueryBuilder = getBoolQueryBuilderRangSearch(boolQueryBuilder,rang_search);
        return boolQueryBuilder;
    }

    /**
     * 根据精确生成布尔条件
     * @param client
     * @param index 索引名称
     * @param boolQueryBuilder es布尔查询条件
     * @param exact_search 精确查询条件
     * @return 返回 boolQueryBuildergetIndexFiledList
     */
    public static BoolQueryBuilder getBoolQueryBuilderExact(RestHighLevelClient client,
                                                            String index,
                                                            BoolQueryBuilder boolQueryBuilder,
                                                            Map<String,String> exact_search) throws Exception{
        // 判断布尔条件
        if(boolQueryBuilder==null){
            boolQueryBuilder = getCommonBoolQueryBuilder();
        }
        if(exact_search==null || exact_search.isEmpty()){
            return boolQueryBuilder;
        }
        // 字段集合
        List<String> fields = EsDataUtil.getIndexFiledListStr(client,index,false);
        // 循环拼接
        for (Map.Entry<String,String> key:exact_search.entrySet()){
            String field = key.getKey();
            String value = key.getValue();
            if(!"".equals(field) && !"".equals(value)){
                /*if(fields.contains(field)){
                    field = field + ".keyword";
                }*/
                boolQueryBuilder.must(QueryBuilders.termQuery(field,value));
            }
        }
        return boolQueryBuilder;
    }

    /**
     * 根据精确生成布尔条件
     * @param client
     * @param index 索引名称
     * @param boolQueryBuilder es布尔查询条件
     * @param search 模糊查询条件
     * @return 返回 boolQueryBuilder
     */
    public static BoolQueryBuilder getBoolQueryBuilderLike(RestHighLevelClient client,
                                                           String index,
                                                           BoolQueryBuilder boolQueryBuilder,
                                                           Map<String,String> search) throws Exception{
        // 判断布尔条件
        if(boolQueryBuilder==null){
            boolQueryBuilder = getCommonBoolQueryBuilder();
        }
        if(search==null || search.isEmpty()){
            return boolQueryBuilder;
        }
        // 字段集合
        List<String> fields = EsDataUtil.getIndexFiledListStr(client,index,false);
        // 循环拼接
        for (Map.Entry<String,String> key:search.entrySet()){
            String field = key.getKey();
            String value = key.getValue();
            if(!"".equals(field) && !"".equals(value)){
                /*if(fields.contains(field)){
                    field = field + ".keyword";
                }*/
                boolQueryBuilder.must(getWildcardQueryBuilder(field,value));
            }
        }
        return boolQueryBuilder;
    }

    /**
     * 根据in查询条件生成布尔条件
     * @param boolQueryBuilder es布尔查询条件
     * @param in_search in查询
     * @return 返回 boolQueryBuilder
     */
    public static BoolQueryBuilder getBoolQueryBuilderIn(BoolQueryBuilder boolQueryBuilder,Map<String, List<String>> in_search) throws Exception{
        // 判断布尔条件
        if(boolQueryBuilder==null){
            boolQueryBuilder = getCommonBoolQueryBuilder();
        }
        if(in_search==null || in_search.isEmpty()){
            return boolQueryBuilder;
        }
        // 循环拼接
        for (Map.Entry<String,List<String>> key:in_search.entrySet()){
            String field = key.getKey();
            List<String> value = key.getValue();
            if(!"".equals(field) && value!=null && value.size()>0){
                boolQueryBuilder.must(QueryBuilders.termsQuery(field,value));
            }
        }
        return boolQueryBuilder;
    }

    /**
     *
     * rang_search:{publish_time:{start:"2020-03-05 00:00:00",end:"2020-03-05 20:47:14",format:'time'}, 字段2：{start:"开始",end:"结束",format:'类型'}}
     * // 说明 ：日期 时间 整数 formate:date 日期，time 时间，number 整数，start  开始参数  end  介绍参数
     * 参数中可以有多组key-value
     * @param boolQueryBuilder
     * @return 返回 BoolQueryBuilder
     * @throws Exception 异常信息
     */

    public static BoolQueryBuilder getBoolQueryBuilderRangSearch(BoolQueryBuilder boolQueryBuilder,
                                                                 Map<String,Map<String,String>> rang_search) throws Exception{
        // 判断布尔条件
        if(boolQueryBuilder==null){
            boolQueryBuilder = getCommonBoolQueryBuilder();
        }
        if(rang_search == null || rang_search.isEmpty()){
            return boolQueryBuilder;
        }
        // 循环 rang_search
        for (Map.Entry<String,Map<String,String>> key:rang_search.entrySet()){
            // 字段
            String field = key.getKey();
            // 字段的参数
            Map<String,String> value = key.getValue();
            String start = value.get("start") == null ? "" : value.get("start").toString();
            String end = value.get("end") == null ? "" : value.get("end").toString();
            String format = value.get("format") == null ? "" : value.get("format").toString();
            // 定义 RangeQueryBuilder 条件
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
            if("number".equals(format)){
                // 开始
                if(!"".equals(start)){
                    int num_start = ParamsUtil.getNumber(field+"_start",start);
                    rangeQueryBuilder.gte(num_start);
                }
                // 结束
                if(!"".equals(end)){
                    int num_end = ParamsUtil.getNumber(field+"_end",end);
                    rangeQueryBuilder.lte(num_end);
                }
            }else if("time".equals(format)||"date".equals(format)||"year".equals(format)||"month".equals(format)){
                String format_time = "";
                // 时间
                if("time".equals(format)){
                    format_time = "yyyy-MM-dd HH:mm:ss";
                }else if("date".equals(format)||"month".equals(format)|| "year".equals(format)){
                    format_time = "yyyy-MM-dd";
                }
                // 校验格式
                // 开始
                if(!"".equals(start)){
                    String str_start = "";
                    if("time".equals(format)||"date".equals(format)){
                        str_start = DateUtil.checkDateStr(field+"_start",start,format_time);
                    }else if("month".equals(format)){
                        String format_time_ = "yyyy-MM";
                        str_start = DateUtil.checkDateStr(field+"_start",start,format_time_);
                        str_start = DateUtil.joinDateFirstDay(str_start,format_time_);
                    }else if("year".equals(format)){
                        String format_time_ = "yyyy";
                        str_start = DateUtil.checkDateStr(field+"_start",start,format_time_);
                        str_start = DateUtil.joinDateFirstDay(str_start,format_time_);
                    }

                    rangeQueryBuilder.gte(str_start);
                }
                // 结束
                if(!"".equals(end)){
                    String str_end = "";
                    if("time".equals(format)||"date".equals(format)){
                        str_end = DateUtil.checkDateStr(field+"_end",end,format_time);
                    }else if("month".equals(format)){
                        String format_time_ = "yyyy-MM";
                        str_end = DateUtil.checkDateStr(field+"_end",end,format_time_);
                        str_end = DateUtil.joinDateLastDay(str_end,format_time_);
                    }else if("year".equals(format)){
                        String format_time_ = "yyyy";
                        str_end = DateUtil.checkDateStr(field+"_end",end,format_time_);
                        str_end = DateUtil.joinDateLastDay(str_end+"-12","yyyy-MM");
                    }
                    rangeQueryBuilder.lte(str_end);
                }
                rangeQueryBuilder.format(format_time);
            }else{
                throw new Exception("rang_search 参数 "+field+" 中format 类型错误");
            }
            boolQueryBuilder.minimumShouldMatch("100%");
            boolQueryBuilder.must(rangeQueryBuilder);
        }
        return boolQueryBuilder;
    }


    /**
     * 生效模糊匹配查询数据
     * @param field 字段
     * @param value 字段值
     * @return 返回 WildcardQueryBuilder
     */
    public static WildcardQueryBuilder getWildcardQueryBuilder(String field, String value){
        return QueryBuilders.wildcardQuery(field,"*"+value+"*");
    }

    /**
     * 获取精确查找和模糊匹配的查询字段
     * @param map 参数  routing
     * @return 返回集合
     */
    public static List<String> getFields(Map<String,Object> map) throws Exception{
        List<String> fields = new ArrayList<>();
        // 精确查询
        Map<String,String>  exact_search = ParamsUtil.getMap(map, "exact_search");
        addList(fields,exact_search);
        // 模糊查询
        Map<String,String> search = ParamsUtil.getMap(map, "search");
        addList(fields,search);
        return fields;
    }

    /**
     * 将 map 中的key放入到list 中
     * @param fields 字段集合
     * @param map 参数
     */
    public static void addList( List<String> fields,Map<String,String>  map){
        if(fields == null){
            fields = new ArrayList<>();
        }
        if(map!=null){
            for (Map.Entry<String,String> key:map.entrySet()){
                String field = key.getKey();
                if(!"".equals(field)&&!"".equals(key.getValue())){
                    if(!fields.contains(field)){
                        fields.add(field);
                    }
                }
            }
        }
    }


    /**
     * 设置高亮显示
     * @param  map 参数
     * @return 返回  HighlightBuilder
     */
    public static HighlightBuilder setHighlightBuilder(Map<String,Object> map) throws Exception{
        List<String> fields = getFields(map);
        return setHighlightBuilder(fields);
    }

    /**
     * 根据需要高亮显示的字段来设置高亮展示
     * @param fields 字段集合
     * @return 返回  HighlightBuilder
     */
    public static HighlightBuilder setHighlightBuilder(List<String> fields){
        // 设置高亮,使用默认的highlighter高亮器
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .preTags("<span style=\"color:red;font-weight:bold;font-size:15px;\">")
                .postTags("</span>");
        for (String field:fields){
            highlightBuilder.field(field);
        }
        return highlightBuilder;
    }

    /**
     * 根据单个字段和值生成查询条件
     * @param field 字段
     * @param value 字段值
     * @return
     */
    public static BoolQueryBuilder getBoolQueryBuilderByFiledValue(String field,String value){
        if("id".equals(field)){
            field = "_id";
        }
        BoolQueryBuilder boolQueryBuilder = getCommonBoolQueryBuilder();
        return getBoolQueryBuilderByFiledValue(boolQueryBuilder,field,value);
    }

    /**
     * 根据单个字段和值集合生成查询条件
     * @param field 字段
     * @param values 字段值集合
     * @return
     */
    public static BoolQueryBuilder getBoolQueryBuilderByFiledValues(String field,List<String> values){
        if("id".equals(field)){
            field = "_id";
        }
        BoolQueryBuilder boolQueryBuilder = getCommonBoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.termsQuery(field,values));
        return boolQueryBuilder;
    }

    /**
     * 根据单个字段和值生成查询条件
     * @param field 字段
     * @param value 字段值
     * @return
     */
    public static BoolQueryBuilder getBoolQueryBuilderByFiledValue(BoolQueryBuilder boolQueryBuilder,String field,String value){
        if("id".equals(field)){
            field = "_id";
        }
        if(boolQueryBuilder == null){
            boolQueryBuilder =  QueryBuilders.boolQuery();
        }
        boolQueryBuilder.must(QueryBuilders.termQuery(field,value));
        return boolQueryBuilder;
    }


    /**
     *
     * @param map
     * @param field
     * @return
     * @throws Exception
     */
    public static Map<String,String> getTime(Map<String,Object> map,String field,String format_) throws Exception {
        //  区间查询
        Map<String, Object> rang_search = new HashMap<>();
        if (map.get("rang_search") != null && !"".equals(map.get("rang_search").toString())) {
            rang_search = (Map<String, Object>) (JSON.parse(map.get("rang_search").toString()));
        }
        Map<String, String> map_ = new HashMap<>();
        // 区间查询
        for (Map.Entry<String, Object> k1 : rang_search.entrySet()) {
            if (k1.getKey() != null && !"".equals(k1.getKey()) && k1.getValue() != null && !"".equals(k1.getValue().toString()) && field.equals(k1.getKey().toString())) {
                JSONObject jsonObject = JSONObject.parseObject(k1.getValue().toString());
                String start = jsonObject.get("start") == null ? "" : jsonObject.get("start").toString();
                String end = jsonObject.get("end") == null ? "" : jsonObject.get("end").toString();
                String str_start = "";
                String str_end = "";

                /*year 按照年份统计
                month 按照月份统计
                day 按照天统计
                            hour 小时*/
                if("year".equals(format_)){
                    str_start = DateUtil.getDateStr(start, "yyyy");
                    str_end = DateUtil.getDateStr(end, "yyyy");
                }else if("month".equals(format_)){
                    str_start = DateUtil.getDateStr(start, "yyyy-MM");
                    str_end = DateUtil.getDateStr(end, "yyyy-MM");
                }else if("day".equals(format_)){
                    str_start = DateUtil.getDateStr(start, "yyyy-MM-dd");
                    str_end = DateUtil.getDateStr(end, "yyyy-MM-dd");
                }else if("hour".equals(format_)){
                    str_start = DateUtil.getDateStr(start, "yyyy-MM-dd HH");
                    str_end = DateUtil.getDateStr(end, "yyyy-MM-dd HH");
                }

                map_.put("min", str_start);
                map_.put("max", str_end);
            }

        }
        return map_;
    }
    // 全文检索生成条件
    public static BoolQueryBuilder getFullTextBoolQueryBuilder(RestHighLevelClient client,
                                                               String q,
                                                               List<String> fields) throws Exception {
        BoolQueryBuilder boolQueryBuilder = getCommonBoolQueryBuilder();
        for (String str:fields){
                // 执行语句
                boolQueryBuilder.should(QueryBuilders.matchQuery(str,q));

        }
        return boolQueryBuilder;
    }
}
