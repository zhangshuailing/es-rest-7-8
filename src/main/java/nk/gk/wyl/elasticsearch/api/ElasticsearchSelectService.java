package nk.gk.wyl.elasticsearch.api;

import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
* @Description:    查询接口
* @Author:         zhangshuailing
* @CreateDate:     2021/1/23 17:02
* @UpdateUser:     zhangshuailing
* @UpdateDate:     2021/1/23 17:02
* @UpdateRemark:   修改内容
* @Version:        1.0
*/
public interface ElasticsearchSelectService {

    /**
     * 根据ids 获取 指定的数据，建议控制ids 数量
     * @param client
     * @param index 索引名称
     * @param ids id集合
     * @param includes 显示字段
     * @return
     * @throws Exception 异常信息
     */
    List<Map> findList(RestHighLevelClient client,
                       String index,List<String> ids,String[] includes) throws Exception;

}
