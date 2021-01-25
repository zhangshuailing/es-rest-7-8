package nk.gk.wyl.elasticsearch.api;

import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
* @Description:    增删改操作接口
* @Author:         zhangshuailing
* @CreateDate:     2021/1/23 17:01
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2021/1/23 17:01
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
public interface ElasticsearchActService {
    /**
     * 批量插入es数据
     * @param client
     * @param index 索引
     * @param list 数据集合
     * @return
     */
    boolean insertBatch(RestHighLevelClient client,
                        String index,
                        List<Map<String,Object>> list);


    /**
     * 批量更新es数据
     * @param client
     * @param index 索引
     * @param list 数据集合
     * @return
     */
    boolean updateBatch(RestHighLevelClient client,
                        String index,
                        List<Map<String,Object>> list);

}
