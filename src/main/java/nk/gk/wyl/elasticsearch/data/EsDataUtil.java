package nk.gk.wyl.elasticsearch.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import nk.gk.wyl.elasticsearch.util.util.ParamsUtil;
import nk.gk.wyl.elasticsearch.util.util.Util;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;

/**
* @Description:    查询结果数据
* @Author:         zhangshuailing
* @CreateDate:     2020/10/15 22:24
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2020/10/15 22:24
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
public class EsDataUtil {

    private static Logger logger = LoggerFactory.getLogger(EsDataUtil.class);

    /**
     * 获取所有索引
     * @param client 实例
     * @return 返回集合数据
     * @throws IOException 异常信息
     */
    public static List<String> getIndices(RestHighLevelClient client) throws IOException {
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesResponse getAliasesResponse =  client.indices().getAlias(request, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetaData>> map = getAliasesResponse.getAliases();
        Set<String> indices = map.keySet();
        List<String> result = new ArrayList<>();
        for (String key : indices) {
            result.add(key);
        }
        return result;
    }
    /**
     * 加载索引中分词的字段【text】
     */
    public static Map<String,Map<String,Object>> index_fields = new HashMap<>();

    /**
     * 加载索引中分词的字段【text】
     */
    public static Map<String,List<String>> index_fields_ = new HashMap<>();


    /**
     * 获取索引对应类型的所有字段
     * @param client
     * @param index 索引名称
     * @return
     */
    public static Map<String, Object> getIndexFiledList(RestHighLevelClient client,
                                                        String index,
                                                        boolean is_all) throws IOException {
        String key = index+"_"+is_all;
        if( index_fields.containsKey(key)){
            return index_fields.get(key);
        }
        GetMappingsRequest getMappings = new GetMappingsRequest().indices(index);
        //调用获取
        GetMappingsResponse getMappingResponse = client.indices().getMapping(getMappings, RequestOptions.DEFAULT);
        //处理数据
        Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();

        for (Map.Entry<String, MappingMetaData> sk:allMappings.entrySet()){
            Map<String, Object> mapProperties = sk.getValue().getSourceAsMap();
            List<Map<String,Object>> list = getIndexFieldList(mapProperties,is_all);
            Map<String,Object> mk = new HashMap<>();
            mk.put(sk.getKey(),list);
            index_fields.put(sk.getKey()+"_"+is_all,mk);
        }
        if(StringUtils.isEmpty(index)){
           // Map<String,Map<String,Object>>
            Map<String,Object> stringObjectMap = new HashMap<>();
            for (Map.Entry<String,Map<String,Object>> sd:index_fields.entrySet()){
                stringObjectMap.putAll(sd.getValue());
            }
            return stringObjectMap;

        }else{
            return index_fields.get(key);
        }
    }

    /**
     * 获取所有的字段
     * @param mapProperties
     * @return
     */
    public static List<Map<String,Object>> getIndexFieldList(Map<String, Object> mapProperties,
                                                             boolean is_all) {
        List<Map<String,Object>> fieldList = new ArrayList<>();
        if(! mapProperties.containsKey("properties")){
            return fieldList;
        }
        Map<String, Object> map = (Map<String, Object>) mapProperties.get("properties");
        for (Map.Entry<String,Object> k:map.entrySet()){
            Map<String,Object> map_field = new HashMap<>();
            String key = k.getKey();
            Map map1 = (Map) k.getValue();
            // fieldList.add(key);
            map_field.put(key,map1);
            if(is_all){
                fieldList.add(map_field);
            }else{
                if(map1.containsKey("analyzer") || map1.containsKey("fields") ){
                    fieldList.add(map_field);
                }
            }
        }
        return fieldList;
    }

    /**
     * 获取索引对应类型的所有字段
     * @param client
     * @param index 索引名称
     * @param is_all true/false
     * @return
     */
    public static List<String> getIndexFiledListStr(RestHighLevelClient client,
                                                    String index,
                                                    boolean is_all) throws Exception {
        String key = index+"_"+is_all;
        if(index_fields_.containsKey(key)){
            return index_fields_.get(key);
        }
        GetMappingsRequest getMappings = new GetMappingsRequest().indices(index);
        //调用获取
        GetMappingsResponse getMappingResponse = client.indices().getMapping(getMappings, RequestOptions.DEFAULT);
        //处理数据
        Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();
        for (Map.Entry<String, MappingMetaData> sk:allMappings.entrySet()){
            Map<String, Object> mapProperties = sk.getValue().getSourceAsMap();
            List<String> list = getIndexFieldListStr(mapProperties,is_all);
            index_fields_.put(sk.getKey()+"_"+is_all,list);
        }

        if(StringUtils.isEmpty(index)){
            // Map<String,Map<String,Object>>
            List<String> list = new ArrayList<>();
            for (Map.Entry<String,List<String>> sd:index_fields_.entrySet()){
                list.addAll(sd.getValue());
            }
            return list;

        }else{
            return index_fields_.get(key);
        }
    }

    /**
     * 获取所有的字段
     * @param mapProperties
     * @return
     */
    public static List<String> getIndexFieldListStr(Map<String, Object> mapProperties,
                                                    boolean is_all) {
        List<String> fieldList = new ArrayList<String>();
        if(! mapProperties.containsKey("properties")){
            return fieldList;
        }
        Map<String, Object> map = (Map<String, Object>) mapProperties
                .get("properties");
        for (Map.Entry<String,Object> k:map.entrySet()){
            String key = k.getKey();
            if("_class".equals(key) || "id".equals(key)){
                continue;
            }
            Map map1 = (Map) k.getValue();
            // fieldList.add(key);
            if(is_all){
                fieldList.add(key);
            }else{
                if(map1.containsKey("analyzer") || map1.containsKey("fields") ){
                    fieldList.add(key);
                }
            }
        }
        return fieldList;
    }


    /**
     * 通过数据编号获取数据信息
     *
     * @param client 实例
     * @param index  索引
     * @param id     数据编号
     * @return 返回镀锡
     * @throws Exception 异常信息
     */
    public static Map<String,Object> findDataById(RestHighLevelClient client,
                                                  String index,
                                                  String id) throws Exception{
        if(!existsIndex(client,index)){
            throw new Exception("索引【"+index+"】不存在");
        }
        GetResponse getResponse = null;
        GetRequest getRequest = new GetRequest(index, id);
        getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        String sourceAsString = getResponse.getSourceAsString();
        if(StringUtils.isEmpty(sourceAsString)){
            return new HashMap<>();
        }
        /**Jackson日期时间序列化问题：
         * Cannot construct instance of `java.time.LocalDateTime` (no Creators, like default constructor, exist): no String-argument constructor/factory method to deserialize from String value ('2020-06-04 15:07:54')
         */
        //                ObjectMapper objectMapper = new ObjectMapper();
        //                objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        //                objectMapper.registerModule(new JavaTimeModule());
        //                T result = objectMapper.readValue(sourceAsString, mappingClass);
        Map<String,Object> result = JSON.parseObject(sourceAsString, Map.class);
        result.put("id",id);
        return result;
    }

    /**
     * 新增数据
     * @param client 实例
     * @param index 索引
     * @param saveOrUpdate 新增的对象
     * @param uid 用户编号
     * @return
     * @throws Exception
     */
    public static String save(RestHighLevelClient client,
                              String index,
                              Map<String, Object> saveOrUpdate,
                              String uid) throws Exception{
        if(!existsIndex(client,index)){
            throw new Exception("索引【"+index+"】不存在");
        }
        Util.addMap(saveOrUpdate,uid);
        // 数据编号
        String id = Util.getResourceId();
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        String dataString = JSONObject.toJSONString(saveOrUpdate);
        request.source(dataString, XContentType.JSON);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("ElasticSearch 创建文档异常：{}", e.getMessage());
            throw new Exception("数据创建失败："+e.getMessage());
        }
        if(response == null){
            throw new Exception("服务器异常");
        }
        return response.getId();
    }

    /**
     * 判断索引是否存在
     *
     * @param client 实例
     * @param index 索引名称
     * @return true/false
     */
    private static boolean existsIndex(RestHighLevelClient client, String index) throws IOException {
        return client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
    }

    /**
     * 更新操作
     * @param client 实例
     * @param index 索引
     * @param saveOrUpdate 更新数据
     * @param uid 用户编号
     * @return true/false
     * @throws Exception
     */
    public static boolean update(RestHighLevelClient client,
                                 String index,
                                 Map<String, Object> saveOrUpdate,
                                 String uid) throws Exception{
        if(!existsIndex(client,index)){
            throw new Exception("索引【"+index+"】不存在");
        }
        String id = ParamsUtil.getValue(saveOrUpdate,"id");
        UpdateResponse updateResponse = null;
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, id);
            String dataString = JSONObject.toJSONString(saveOrUpdate);
            updateRequest.doc(dataString, XContentType.JSON);
            updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("ElasticSearch 更新文档异常：{}", e.getMessage());
            throw new Exception("数据更新失败："+e.getMessage());
        }
        int status = updateResponse.status().getStatus();
        if(status == 200){
            return  true;
        }else if(status == 404){
            throw new Exception("数据不存在");
        }else{
            return false;
        }
    }


    /**
     * 删除数据
     *
     * @param client 实例
     * @param index 索引名称
     * @param id     数据编号
     * @param uid    用户编号
     * @return 返回 true/false
     * @throws Exception 异常信息
     */
    public static boolean delete(RestHighLevelClient client, String index, String id, String uid) throws Exception {
        if(!existsIndex(client,index)){
            throw new Exception("索引【"+index+"】不存在");
        }
        DeleteResponse deleteResponse = null;
        try {
            DeleteRequest deleteRequest = new DeleteRequest(index, id);
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("ElasticSearch 删除文档异常：{}", e.getMessage());
            throw new Exception("数据删除失败："+e.getMessage());
        }
        int status = deleteResponse.status().getStatus();
        if(status == 200){
            return  true;
        }else if(status == 404){
            throw new Exception("数据不存在");
        }else{
            return false;
        }
    }

    /**
     * 批量删除数据
     *
     * @param client 实例
     * @param index 索引名称
     * @param ids    数据编号集合
     * @param uid    用户编号
     * @return 返回 true/false
     * @throws Exception 异常信息
     */
    public static boolean delete(RestHighLevelClient client, String index, List<String> ids, String uid) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : ids) {
            DeleteRequest deleteRequest = new DeleteRequest(index, id);
            bulkRequest.add(deleteRequest);
        }
        BulkResponse bulk = null;
        try {
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("ElasticSearch批量删除文档信息异常：{}", e.getMessage());
        }
        return bulk != null && !bulk.hasFailures();
    }

    /**
     * 批量插入
     * @param client 实例
     * @param index 索引
     * @param list 数据集合
     * @param uid 用户编号
     */
    public static boolean  batchInsert(RestHighLevelClient client, String index, List<Map> list, String uid){
        BulkRequest bulkRequest = new BulkRequest();
        for (Map obj : list) {
            // 自动生成id
            obj.put("id",Util.getResourceId());
            Util.addMap(obj,uid);
            bulkRequest.add(new IndexRequest(index).source(JSON.toJSONString(obj), XContentType.JSON));
        }
        BulkResponse bulk = null;
        try {
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("ElasticSearch批量操作文档信息异常：{}", e.getMessage());
        }
        return bulk != null && !bulk.hasFailures();
    }

    /**
     * 生成 SearchRequest
     * @param index
     * @param searchSourceBuilder
     * @return
     */
    public static SearchRequest getSearchRequest(RestHighLevelClient client, String index, SearchSourceBuilder searchSourceBuilder) throws Exception {
        SearchRequest searchRequest = null;
        if(StringUtils.isEmpty(index)){
            List<String> fields = getIndices(client);
            String[] keys = new String[fields.size()];
            fields.toArray(keys);
            searchRequest = new SearchRequest(keys);
        }else{
            searchRequest = new SearchRequest(index);
        }
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    /**
     * 不分页列表查询
     * @param client 实例
     * @param index 索引名称
     * @param boolQueryBuilder 查询条件
     * @param highlightBuilder 高亮显示
     * @param includes 显示字段数组
     * @param excludes 隐藏字段数组
     * @param order 排序 {key:value} value int  1 升序 1 降序
     * @return
     * @throws IOException
     */
    public static List<Map> findList(RestHighLevelClient client,
                                     String index,
                                     BoolQueryBuilder boolQueryBuilder,
                                     HighlightBuilder highlightBuilder,
                                     String[] includes,
                                     String[] excludes,
                                     Map<String,Integer> order) throws Exception {
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(boolQueryBuilder,highlightBuilder,includes,excludes,order,-1);
        SearchRequest searchRequest = getSearchRequest(client,index,searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(10));
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map> result = new ArrayList<>();
        dealData(result,response);
        // 注：处理结束后，记得clean scroll
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(response.getScrollId());
        while (response.getHits().getHits().length>0) {
            String scrollId = response.getScrollId();
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(10));
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            dealData(result,response);
            request.addScrollId(response.getScrollId());
        }
        client.clearScroll(request, RequestOptions.DEFAULT);
        logger.info("结果数量:"+result.size());
        return result;
    }

    /**
     *  生成查询条件【SearchRequestBuilder】
     * @param boolQueryBuilder 布尔条件
     * @param highlightBuilder 高亮显示
     * @return SearchRequestBuilder对象
     */
    public static SearchSourceBuilder getSearchSourceBuilder(BoolQueryBuilder boolQueryBuilder,
                                                             HighlightBuilder highlightBuilder,
                                                             String[] includes,
                                                             String[] excludes,
                                                             Map<String,Integer> order,
                                                             int pageSize){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置查询条件
        searchSourceBuilder.query(boolQueryBuilder);
        // 设置高亮显示
        searchSourceBuilder.highlighter(highlightBuilder);
        //设置是否按匹配度排序
        searchSourceBuilder.explain(true);
        // 显示或隐藏字段
        searchSourceBuilder.fetchSource(includes,excludes);
        // 排序
        for (Map.Entry<String,Integer> key:order.entrySet()){
            if(key.getValue()==1){
                searchSourceBuilder.sort(new FieldSortBuilder(key.getKey()).order(SortOrder.ASC));
            }else{
                searchSourceBuilder.sort(new FieldSortBuilder(key.getKey()).order(SortOrder.DESC));
            }
        }
        if(pageSize!=-1){
            searchSourceBuilder.size(pageSize);
        }else{
            searchSourceBuilder.size(1000);
        }
        return searchSourceBuilder;
    }

    /**
     * 根据响应结果获取列表数据
     * @param result 列表集合
     * @param response 响应结果
     * @return 列表数据
     */
    public static void dealData(List<Map> result, SearchResponse response){
        // 返回列表集合
        if(result == null){
            result = new ArrayList<>();
        }
        SearchHits hits = response.getHits();
        for (SearchHit hit:hits.getHits()) {
            Map map1 = hit.getSourceAsMap();
            map1.put("id",hit.getId());
            map1.put("_index",hit.getIndex());
            setHighlightField(map1,hit);
            result.add(map1);
        }
    }

    /**
     * 设置高亮字段.
     * @param map 参数
     * @param hit
     */
    public static void setHighlightField(Map<String,Object> map, SearchHit hit){
        //获取高亮字段
        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
        for (Map.Entry<String, HighlightField> key:highlightFields.entrySet()){
            HighlightField field = highlightFields.get(key.getKey());
            //千万记得要记得判断是不是为空,不然你匹配的第一个结果没有高亮内容,那么就会报空指针异常,这个错误一开始真的搞了很久
            if(field!=null){
                Text[] fragments = field.fragments();
                String name = "";
                for (Text text : fragments) {
                    name+=text;
                }
                map.put(key.getKey(), name);   //高亮字段替换掉原本的内容
            }
        }
    }

    public static SearchResponse getSearchResponse(SearchRequestBuilder searchRequestBuilder, @Nullable String[]includes, @Nullable String[] excludes ){
        return searchRequestBuilder.setFetchSource(includes,excludes).execute().actionGet();
    }


    /**
     * 分页列表数据查询
     * @param client client
     * @param index 索引名称
     * @param pageNo 页码
     * @param pageSize 每页显示数量
     * @param order 排序
     * @param boolQueryBuilder 布尔条件
     * @param highlightBuilder 高亮显示
     * @param includes  包含字段
     * @param excludes  排除的字段
     * @return 返回分页对象
     */
    public static Map<String,Object> getPage(RestHighLevelClient client,
                                             String index,
                                             int pageNo,
                                             int pageSize,
                                             Map<String,Integer> order,
                                             BoolQueryBuilder boolQueryBuilder,
                                             HighlightBuilder highlightBuilder,
                                             String[] includes,
                                             String[] excludes) throws Exception {
        logger.info("布尔条件语句："+boolQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(boolQueryBuilder,highlightBuilder,includes,excludes,order,pageSize);
        SearchRequest searchRequest = getSearchRequest(client,index,searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(10));
        // 获取数据
        Map<String,Object> list = getDataPage(client,searchRequest,pageNo,pageSize);
        return list;
    }

    /**
     * 分页获取数据
     * @param client 实例
     * @param searchRequest
     * @param pageNo 页码
     * @param pageSize 每页显示数据量
     * @return
     * @throws Exception
     */
    public static Map<String,Object> getDataPage(RestHighLevelClient client,
                                                 SearchRequest searchRequest,
                                                 int pageNo,
                                                 int pageSize) throws Exception{
        // 定义返回值
        Map<String,Object> result = new HashMap<>();
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map> list = new ArrayList<>();

        // 注：处理结束后，记得clean scroll
        ClearScrollRequest request = new ClearScrollRequest();

        long totalCount = response.getHits().getTotalHits().value;
        if(pageNo == 1){
            dealData(list,response);
            request.addScrollId(response.getScrollId());
        }else{
            //此处实现迭代的方式较多，仅列其一，
            int j = 1;
            while (response.getHits().getHits().length>0) {
                String scrollId = response.getScrollId();
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueSeconds(10));
                response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                if((j+1)==pageNo){
                    dealData(list,response);
                    request.addScrollId(response.getScrollId());
                    // 跳出查询
                    break;
                }
                j++;
                System.out.println(j);
            }

        }
        client.clearScroll(request, RequestOptions.DEFAULT);
        Map<String,Integer> pager = new HashMap<>();
        pager.put("total", new Long(totalCount).intValue());
        pager.put("start", (pageNo - 1) * pageSize);
        pager.put("limit", pageSize);
        result.put("pager",pager);
        result.put("data",list);
        return result;
    }
}
