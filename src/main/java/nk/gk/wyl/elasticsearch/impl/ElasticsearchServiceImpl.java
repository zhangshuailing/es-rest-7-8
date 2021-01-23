package nk.gk.wyl.elasticsearch.impl;

import nk.gk.wyl.elasticsearch.api.ElasticsearchService;
import nk.gk.wyl.elasticsearch.data.EsDataUtil;
import nk.gk.wyl.elasticsearch.util.util.ParamsUtil;
import nk.gk.wyl.elasticsearch.util.util.QueryUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ElasticsearchServiceImpl implements ElasticsearchService {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchServiceImpl.class);
    /**
     * 新增或编辑数据
     *
     * @param client
     * @param index
     * @param saveOrUpdate
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public String saveOrUpdate(RestHighLevelClient client,
                               String index,
                               Map<String, Object> saveOrUpdate,
                               String uid) throws Exception {
        if(StringUtils.isEmpty(saveOrUpdate)){
            throw new Exception("参数【saveOrUpdate】不能为空");
        }
        String id = "";
        if(saveOrUpdate.containsKey("id")){
            boolean bl = EsDataUtil.update(client,index,saveOrUpdate,uid);
            if(bl){
                id = ParamsUtil.getValue(saveOrUpdate,"id");
            }
        }else{
            id = EsDataUtil.save(client,index,saveOrUpdate,uid);
        }
        logger.info("结果编号："+id);
        return id;
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
    @Override
    public Map<String, Object> findDataById(RestHighLevelClient client, String index, String id) throws Exception {
        return EsDataUtil.findDataById(client, index, id);
    }

    /**
     * 分页列表
     *
     * @param client 实例
     * @param index  索引
     * @param map    参数
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    @Override
    public Map<String, Object> page(RestHighLevelClient client, String index, Map<String, Object> map) throws Exception {
        // 获取是否有指定的显示或隐藏字段
        Map<String, Integer> fields = ParamsUtil.getMapInteger(map, "fields");
        return page(client,index,map,fields);
    }

    /**
     * 分页列表
     *
     * @param client 实例
     * @param index  索引
     * @param map    参数
     * @param fields 显示或者隐藏的字段 value 1 显示  0 隐藏
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    @Override
    public Map<String, Object> page(RestHighLevelClient client, String index, Map<String, Object> map, Map<String, Integer> fields) throws Exception {
        // 默认第一页
        int pageNo = Integer.parseInt(map.get("pageNo") == null ?"1":map.get("pageNo").toString());
        // 默认每页展示10条
        int pageSize = Integer.parseInt(map.get("pageSize") == null ?"10":map.get("pageSize").toString());
        return page(client,index,pageNo,pageSize,map,fields);
    }

    /**
     * @param client   实例
     * @param index    索引
     * @param pageNo   页面
     * @param pageSize 没有显示数据量
     * @param map      参数
     * @param fields   显示字段 value  1 显示 0 隐藏
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    @Override
    public Map<String, Object> page(RestHighLevelClient client, String index, int pageNo, int pageSize, Map<String, Object> map, Map<String, Integer> fields) throws Exception {
        // 排序
        Map<String,Integer> order = ParamsUtil.getMapInteger(map,"order");
        // 查询条件
        BoolQueryBuilder boolQueryBuilder = QueryUtil.getBoolQueryBuilder(client,index,map);
        return page(client,index,pageNo,pageSize,map,boolQueryBuilder,order,fields);
    }

    /**
     * 分页列表
     *
     * @param client           实例
     * @param index            索引
     * @param pageNo           页面
     * @param pageSize         没有显示数据量
     * @param map              参数
     * @param boolQueryBuilder 布尔条件
     * @param order            排序 value  1 升序 -1 降序
     * @param fields           显示字段 value  1 显示 0 隐藏
     * @return 返回对象
     * @throws Exception 异常信息
     */
    @Override
    public Map<String, Object> page(RestHighLevelClient client, String index, int pageNo, int pageSize, Map<String, Object> map, BoolQueryBuilder boolQueryBuilder, Map<String, Integer> order, Map<String, Integer> fields) throws Exception {
        List<String> list_show = new ArrayList<>();
        List<String> list_hide = new ArrayList<>();
        for (Map.Entry<String,Integer> key:fields.entrySet()){
            if(key.getValue()==1){
                list_show.add(key.getKey());
            }else{
                list_hide.add(key.getKey());
            }
        }
        // 显示
        String[] includes = ParamsUtil.listToArray(list_show);
        // 隐藏
        String[] excludes = ParamsUtil.listToArray(list_hide);
        String q = ParamsUtil.getValue(map,"q");
        if(boolQueryBuilder==null){
            boolQueryBuilder = QueryUtil.getCommonBoolQueryBuilder();
        }
        HighlightBuilder highlightBuilder = null;
        if(!StringUtils.isEmpty(q)){// 全文检索
            List<String> keys = ParamsUtil.getArrayList(map,"keys");
            if(keys==null || keys.isEmpty()){

                List<String> field_s = EsDataUtil.getIndexFiledListStr(client,index,false);
                keys = new ArrayList<>();
                for (String str:field_s){
                    if(!keys.contains(str)){
                        keys.add(str);
                    }
                }
            }
            boolQueryBuilder.must(QueryUtil.getFullTextBoolQueryBuilder(client,q,keys));
            // 高亮显示设置
            if(map.containsKey("is_high") && "true".equals(map.get("is_high").toString())){
                highlightBuilder = QueryUtil.setHighlightBuilder(keys);
            }
        }
        return EsDataUtil.getPage(client,index,pageNo,pageSize,order,boolQueryBuilder,highlightBuilder,includes,excludes);
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
    @Override
    public boolean delete(RestHighLevelClient client,String index, String id, String uid) throws Exception {
        return EsDataUtil.delete(client,index,id,uid);
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
    @Override
    public boolean delete(RestHighLevelClient client,String index,List<String> ids, String uid) throws Exception {
        return EsDataUtil.delete(client,index,ids,uid);
    }

    /**
     * 不分页列表
     *
     * @param client 实例
     * @param index 索引
     * @param map   参数
     * @return 返回集合数据
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, Map<String, Object> map) throws Exception {
        List<String> list_show = new ArrayList<>();
        List<String> list_hide = new ArrayList<>();
        // 获取是否有指定的显示或隐藏字段
        Map<String, Integer> fields = ParamsUtil.getMapInteger(map, "fields");
        for (Map.Entry<String,Integer> key:fields.entrySet()){
            if(key.getValue()==1){
                list_show.add(key.getKey());
            }else{
                list_hide.add(key.getKey());
            }
        }
        // 显示
        String[] includes = ParamsUtil.listToArray(list_show);
        // 隐藏
        String[] excludes = ParamsUtil.listToArray(list_hide);
        return findList(client,index,map,includes,excludes);
    }

    /**
     * 列表【增加显示或隐藏字段参数】
     *
     * @param client   实例
     * @param index    索引
     * @param map      参数
     * @param includes 包含字段
     * @param excludes 排除的字段
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, Map<String, Object> map, String[] includes, String[] excludes) throws Exception {
        // 排序
        Map<String,Integer> order = ParamsUtil.getMapInteger(map,"order");
        return findList(client,index,map,order,includes,excludes);
    }

    /**
     * 列表【排序，字段显示或隐藏参数】
     *
     * @param client   实例
     * @param index    索引
     * @param map      参数
     * @param order    排序 value  1 升序 -1 降序
     * @param includes 包含字段
     * @param excludes 排除的字段
     * @return 返回对象
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, Map<String, Object> map, Map<String, Integer> order, String[] includes, String[] excludes) throws Exception {
        // 高亮显示设置
        HighlightBuilder highlightBuilder = null;
        if(map.containsKey("is_high") && "true".equals(map.get("is_high").toString())){
            highlightBuilder = QueryUtil.setHighlightBuilder(map);
        }
        // 查询条件
        BoolQueryBuilder boolQueryBuilder = QueryUtil.getBoolQueryBuilder(client,index,map);
        return EsDataUtil.findList(client,index,boolQueryBuilder,highlightBuilder,includes,excludes,order);
    }

    /**
     * 列表【基于 BoolQueryBuilder 查询条件】
     *
     * @param client           实例
     * @param index            索引
     * @param boolQueryBuilder 布尔条件
     * @param includes         包含字段
     * @param excludes         排除的字段
     * @return 返回列表数据
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, BoolQueryBuilder boolQueryBuilder, String[] includes, String[] excludes) throws Exception {
        return EsDataUtil.findList(client,index,boolQueryBuilder,null,includes,excludes,null);
    }

    /**
     * 列表【基于 BoolQueryBuilder 查询条件】
     *
     * @param client           实例
     * @param index            索引
     * @param boolQueryBuilder 布尔条件
     * @return 返回列表数据
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, BoolQueryBuilder boolQueryBuilder) throws Exception {
        return findList(client,index,boolQueryBuilder,null,null);
    }

    /**
     * 列表【基于 exact_search 精确查找】
     *
     * @param client       实例
     * @param exact_search 精确查找
     * @param index        索引名称
     * @return 返回集合
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, Map<String, String> exact_search, String index) throws Exception {
        BoolQueryBuilder boolQueryBuilder = QueryUtil.getBoolQueryBuilderExact(client,index,null,exact_search);
        return findList(client,index,boolQueryBuilder,null,null);
    }

    /**
     * 列表【基于单个字段和单个值 精确查找】
     *
     * @param client 实例
     * @param index  索引名称
     * @param field  字段
     * @param value  字段值
     * @return 返回集合
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, String field, String value) throws Exception {
        BoolQueryBuilder boolQueryBuilder = QueryUtil.getBoolQueryBuilderByFiledValue(field,value);
        return findList(client,index,boolQueryBuilder,null,null);
    }

    /**
     * 列表【基于单个字段和单个值集合 精确查找】
     *
     * @param client 实例
     * @param index  索引名称
     * @param field  字段
     * @param values 字段值集合
     * @return 返回集合
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, String field, List<String> values) throws Exception {
        BoolQueryBuilder boolQueryBuilder = QueryUtil.getBoolQueryBuilderByFiledValues(field,values);
        return findList(client,index,boolQueryBuilder,null,null);
    }
}
