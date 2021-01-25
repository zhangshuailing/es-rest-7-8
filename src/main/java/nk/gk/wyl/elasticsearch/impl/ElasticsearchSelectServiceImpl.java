package nk.gk.wyl.elasticsearch.impl;

import nk.gk.wyl.elasticsearch.api.ElasticsearchSelectService;
import nk.gk.wyl.elasticsearch.api.ElasticsearchService;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
* @Description:    查询接口实现类
* @Author:         zhangshuailing
* @CreateDate:     2021/1/23 17:05
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2021/1/23 17:05
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
@Service
public class ElasticsearchSelectServiceImpl implements ElasticsearchSelectService {

    @Autowired
    private ElasticsearchService elasticsearchService;
    /**
     * 根据ids 获取 指定的数据，建议控制ids 数量
     *
     * @param client
     * @param index  索引名称
     * @param ids    id集合
     * @param includes 显示字段
     * @return
     * @throws Exception 异常信息
     */
    @Override
    public List<Map> findList(RestHighLevelClient client, String index, List<String> ids,String[] includes) throws Exception {
        Map<String, List<String>> in_search = new HashMap<>();
        in_search.put("_id",ids);
        Map<String,Object> map = new HashMap<>();
        map.put("in_search",in_search);
        List<Map> list = elasticsearchService.findList(client,index,map,includes,null);
        return list;
    }
}
