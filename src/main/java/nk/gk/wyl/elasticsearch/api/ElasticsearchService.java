package nk.gk.wyl.elasticsearch.api;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.List;
import java.util.Map;

/**
* @Description:    接口
* @Author:         zhangshuailing
* @CreateDate:     2021/1/23 12:32
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2021/1/23 12:32
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
public interface ElasticsearchService {
    /**
     * 新增或编辑数据
     * @param client
     * @param index
     * @param saveOrUpdate
     * @param uid
     * @return
     * @throws Exception
     */
    public String saveOrUpdate(RestHighLevelClient client,
                               String index,
                               Map<String,Object> saveOrUpdate,
                               String uid) throws Exception;
    /**
     * 通过数据编号获取数据信息
     * @param client 实例
     * @param index 索引
     * @param id 数据编号
     * @return 返回镀锡
     * @throws Exception 异常信息
     */
    public Map<String,Object> findDataById(RestHighLevelClient client,
                                           String index,
                                           String id) throws Exception;

    /**
     * 分页列表
     *
     * @param client 实例
     * @param index 索引
     * @param map   参数
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    public Map<String, Object> page(RestHighLevelClient client, String index, Map<String, Object> map) throws Exception;

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
    public Map<String, Object> page(RestHighLevelClient client,String index,Map<String, Object> map, Map<String, Integer> fields) throws Exception;

    /**
     *
     * @param client 实例
     * @param index 索引
     * @param pageNo 页面
     * @param pageSize 没有显示数据量
     * @param map 参数
     * @param fields 显示字段 value  1 显示 0 隐藏
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    public Map<String, Object> page(RestHighLevelClient client,String index,int pageNo, int pageSize, Map<String, Object> map, Map<String, Integer> fields) throws Exception;

    /**
     * 分页列表
     *
     * @param client 实例
     * @param index 索引
     * @param pageNo 页面
     * @param pageSize 没有显示数据量
     * @param map 参数
     * @param boolQueryBuilder 布尔条件
     * @param order 排序 value  1 升序 -1 降序
     * @param fields 显示字段 value  1 显示 0 隐藏
     * @return 返回对象
     * @throws Exception 异常信息
     */
    public Map<String,Object> page(RestHighLevelClient client, String index, int pageNo, int pageSize, Map<String, Object> map, BoolQueryBuilder boolQueryBuilder, Map<String,Integer> order, Map<String, Integer> fields) throws Exception;

    /**
     * 删除数据
     * @param client 实例
     * @param index 索引名称
     * @param id 数据编号
     * @param uid 用户编号
     * @return 返回 true/false
     * @throws Exception 异常信息
     */
    public boolean delete(RestHighLevelClient client,String index,String id,String uid) throws Exception;

    /**
     * 批量删除数据
     * @param client 实例
     * @param index 索引名称
     * @param ids 数据编号集合
     * @param uid 用户编号
     * @return 返回 true/false
     * @throws Exception 异常信息
     */
    public boolean delete(RestHighLevelClient client, String index, List<String> ids, String uid) throws Exception;





    /**
     * 列表【基于 map 参数】
     *
     * @param client 实例
     * @param index 索引
     * @param map   参数
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              Map<String, Object> map) throws Exception;

    /**
     * 列表【增加显示或隐藏字段参数】
     *
     * @param client  实例
     * @param index  索引
     * @param map    参数
     * @param includes  包含字段
     * @param excludes  排除的字段
     * @return 返回对象数据
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              Map<String, Object> map,
                              String[] includes,
                              String[] excludes) throws Exception;

    /**
     * 列表【排序，字段显示或隐藏参数】
     * @param client  实例
     * @param index 索引
     * @param map 参数
     * @param order 排序 value  1 升序 -1 降序
     * @param includes  包含字段
     * @param excludes  排除的字段
     * @return 返回对象
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              Map<String, Object> map,
                              Map<String,Integer> order,
                              String[] includes,
                              String[] excludes) throws Exception;

    /**
     * 列表【基于 BoolQueryBuilder 查询条件】
     * @param client  实例
     * @param index 索引
     * @param boolQueryBuilder 布尔条件
     * @param includes  包含字段
     * @param excludes  排除的字段
     * @return 返回列表数据
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              BoolQueryBuilder boolQueryBuilder,
                              String[] includes,
                              String[] excludes) throws Exception;


    /**
     * 列表【基于 BoolQueryBuilder 查询条件】
     * @param client  实例
     * @param index 索引
     * @param boolQueryBuilder 布尔条件
     * @return 返回列表数据
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              BoolQueryBuilder boolQueryBuilder) throws Exception;


    /**
     * 列表【基于 exact_search 精确查找】
     * @param client  实例
     * @param exact_search 精确查找
     * @param index 索引名称
     * @return 返回集合
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              Map<String,String> exact_search,
                              String index) throws Exception;

    /**
     * 列表【基于单个字段和单个值 精确查找】
     * @param client  实例
     * @param index 索引名称
     * @param field 字段
     * @param value 字段值
     * @return 返回集合
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              String field,
                              String value) throws Exception;

    /**
     *  列表【基于单个字段和单个值集合 精确查找】
     * @param client  实例
     * @param index 索引名称
     * @param field 字段
     * @param values 字段值集合
     * @return 返回集合
     * @throws Exception 异常信息
     */
    public List<Map> findList(RestHighLevelClient client,
                              String index,
                              String field,
                              List<String> values) throws Exception;
}
