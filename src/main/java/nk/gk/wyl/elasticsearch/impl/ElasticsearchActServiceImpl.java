package nk.gk.wyl.elasticsearch.impl;

import com.alibaba.fastjson.JSONObject;
import nk.gk.wyl.elasticsearch.api.ElasticsearchActService;
import nk.gk.wyl.elasticsearch.util.util.Util;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
* @Description:    java类作用描述
* @Author:         zhangshuailing
* @CreateDate:     2021/1/23 18:37
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2021/1/23 18:37
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
@Service
public class ElasticsearchActServiceImpl implements ElasticsearchActService {
    Logger logger = LoggerFactory.getLogger(ElasticsearchActServiceImpl.class);

    /**
     * 批量插入es数据
     *
     * @param client
     * @param index  索引
     * @param list   数据集合
     * @return
     */
    @Override
    public boolean insertBatch(RestHighLevelClient client, String index, List<Map<String,Object>> list) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String,Object> map : list) {
            IndexRequest request = new IndexRequest(index);
            String id = map.get("id") == null ? Util.getResourceId() : map.get("id").toString();
            request.id(id);
            map.remove("id");
            String dataString = JSONObject.toJSONString(map);
            request.source(dataString, XContentType.JSON);
            bulkRequest.add(request);

        }
        BulkResponse bulk = null;
        try {
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            return bulk != null && !bulk.hasFailures();
        } catch (Exception e) {
            logger.error("ElasticSearch批量插入文档信息异常：{}", e.getMessage());
            return false;
        }
    }

    /**
     * 批量更新es数据
     *
     * @param client
     * @param index  索引
     * @param list   数据集合
     * @return
     */
    @Override
    public boolean updateBatch(RestHighLevelClient client, String index, List<Map<String, Object>> list) {
        BulkRequest bulkRequest = new BulkRequest();
        int size = 0;
        for (int i = 0; i < list.size(); i++) {
            Map<String,Object> saveOrUpdate = list.get(i);
            String id = saveOrUpdate.get("id") == null ? "" : saveOrUpdate.get("id").toString();
            if("".equals(id)){
                continue;
            }
            size +=1;
            UpdateRequest updateRequest = new UpdateRequest(index, id);
            String dataString = JSONObject.toJSONString(saveOrUpdate);
            updateRequest.doc(dataString, XContentType.JSON);
            bulkRequest.add(updateRequest);
        }
        if(size == 0){
            return false;
        }
        BulkResponse bulk = null;
        try {
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            return bulk != null && !bulk.hasFailures();
        } catch (Exception e) {
            logger.error("ElasticSearch批量更新文档信息异常：{}", e.getMessage());
            return false;
        }
    }
}
