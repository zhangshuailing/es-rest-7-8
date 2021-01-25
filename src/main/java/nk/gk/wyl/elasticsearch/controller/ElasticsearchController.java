package nk.gk.wyl.elasticsearch.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import nk.gk.wyl.elasticsearch.api.ElasticsearchActService;
import nk.gk.wyl.elasticsearch.api.ElasticsearchService;
import nk.gk.wyl.elasticsearch.data.EsDataUtil;
import nk.gk.wyl.elasticsearch.entity.result.Response;
import nk.gk.wyl.elasticsearch.util.util.ParamsUtil;
import nk.gk.wyl.elasticsearch.util.util.Util;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("rest/v1/elasticsearch")
@Api(value = "Elasticsearch 7.8.1接口", tags = "Elasticsearch 7.8.1接口")
public class ElasticsearchController {
    @GetMapping("")
    @ApiOperation(value = "index")
    public @ResponseBody
    Response index(){
        return new Response().success(Util.getStrDate());
    }

    @Autowired
    private RestHighLevelClient client;

    public RestHighLevelClient getClient() {
        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    @Autowired
    private ElasticsearchService api;
    @Autowired
    private ElasticsearchActService actApi;

    /**
     * 新增或编辑
     * @param index 索引
     * @param body 参数
     * @return 信息或编辑的树编号
     */
    @PostMapping("{index}/saveOrUpdate")
    @ApiOperation(value = "新增或编辑")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、saveOrUpdate {field:value[Object]}</br>" +
                    "2、uid 当前登录账号信息 String",required = true,defaultValue = "{}")
    })
    public @ResponseBody
    Response saveOrUpdate(@PathVariable("index") String index,
                               @RequestBody Map<String,Object> body){
        Map<String,Object> saveOrUpdate = null;
        try {
            saveOrUpdate = ParamsUtil.checkMap(body,"saveOrUpdate");
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
        String uid = ParamsUtil.getUid(body);
        try {
            return new Response().success(api.saveOrUpdate(client,index,saveOrUpdate,uid));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 根据数据编号获取数据信息
     * @param index 索引名称
     * @param id 数据编号
     * @return 返回数据对象
     */
    @GetMapping("{index}/{id}")
    @ApiOperation(value = "根据数据编号【id】获取单条数据信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index",value = "索引名称",required = true,paramType = "path"),
            @ApiImplicitParam(name = "id",value = "数据编号",required = true,paramType = "path"),
    })
    public @ResponseBody
    Response  findDataById(@PathVariable("index") String index,
                                           @PathVariable("id") String id) {
        try {
            return new Response().success(api.findDataById(client,index,id));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }


    /**
     * 单个删除数据
     * @param index 索引
     * @param id 数据编号
     * @param uid 用户数据编号
     * @return 返回对象数据
     */
    @DeleteMapping("{index}/{id}")
    @ApiOperation(value = "删除单条数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "id", value = "数据编号", dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "uid", value = "用户编号", dataType="string",defaultValue = "",paramType = "query")
    })
    public @ResponseBody
    Response  delete(@PathVariable("index") String index,
                          @PathVariable("id") String id,
                          @RequestParam(value = "uid",defaultValue = "09901") String uid) {
        try {
            return new Response().success(api.delete(client,index,id,uid));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 列表
     * @param index 索引
     * @param body 参数
     * @return 返回分页对象
     */
    @PostMapping("{index}/list")
    @ApiOperation(value = "不分页列表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、fields 字段显示或隐藏 {filed:int[1 显示 0隐藏]}</br>" +
                    "2、is_high 是否高亮显示 true/false</br>" +
                    "3、order 排序{filed:int[1升序-1降序]}</br>" +
                    "4、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "5、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "6、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "7、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "8、uid 当前登录账号信息 String",required = true,defaultValue = "{}")
    })
    public @ResponseBody
    Response  list(@PathVariable("index") String index,
                          @RequestBody Map<String,Object> body) {
        try {
            return new Response().success(api.findList(client,index,body));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 分页列表
     * @param index 索引
     * @param body 参数
     * @return 返回分页对象
     */
    @PostMapping("{index}/page")
    @ApiOperation(value = "分页列表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、pageNo 当前页面【int】</br>" +
                    "2、pageSize 每页默认显示条数【int】</br>" +
                    "3、fields 字段显示或隐藏 {filed:int[1 显示 0隐藏]}</br>" +
                    "4、is_high 是否高亮显示 true/false</br>" +
                    "5、order 排序{filed:int[1升序-1降序]}</br>" +
                    "6、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "7、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "8、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "9、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "10、uid 当前登录账号信息 String<br>" +
                    "11、q 全文检索<br>" +
                    "12、keys 指定分词的字段",required = true,defaultValue = "{}")
    })
    public @ResponseBody
    Response  page(@PathVariable("index") String index,
                                   @RequestBody Map<String,Object> body) {
        try {
            return new Response().success(api.page(client,index,body));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 分页列表【支持全文检索】
     * @param body 参数
     * @return 返回分页对象
     */
    @PostMapping("pageFull")
    @ApiOperation(value = "分页列表【支持全文检索】")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、pageNo 当前页面【int】</br>" +
                    "2、pageSize 每页默认显示条数【int】</br>" +
                    "3、fields 字段显示或隐藏 {filed:int[1 显示 0隐藏]}</br>" +
                    "4、is_high 是否高亮显示 true/false</br>" +
                    "5、order 排序{filed:int[1升序-1降序]}</br>" +
                    "6、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "7、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "8、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "9、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "10、uid 当前登录账号信息 String<br>" +
                    "11、q 全文检索<br>" +
                    "12、keys 指定分词的字段",required = true,defaultValue = "{}")
    })
    public @ResponseBody
    Response  pageFull(@RequestBody Map<String,Object> body) {
        String index = ParamsUtil.getValue(body,"index");
        try {
            return new Response().success(api.page(client,index,body));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }
    /**
     * 根据索引名称获取字段
     * @param index 索引名称
     * @param is_all 是否显示全部字段 true/false【分词】
     * @return 返回集合数据
     */
    @GetMapping("{index}/field")
    @ApiOperation(value = "根据索引名称获取字段集合")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index",value = "索引名称",required = true,paramType = "path"),
            @ApiImplicitParam(name = "is_all",value = "是否显示全部字段 true/false【分词】",defaultValue = "true",paramType = "query"),
    })
    public @ResponseBody
    Response  findIndexFiledList(@PathVariable("index") String index,
                                                 @RequestParam(value = "is_all",defaultValue = "true") boolean is_all) {
        try {
            return new Response().success(EsDataUtil.getIndexFiledList(client,index,is_all));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 根据索引名称获取字段
     * @param is_all 是否显示全部字段 true/false【分词】
     * @return 返回集合数据
     */
    @GetMapping("fieldAll")
    @ApiOperation(value = "根据索引名称获取字段集合")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "is_all",value = "是否显示全部字段 true/false【分词】",defaultValue = "true",paramType = "query"),
    })
    public @ResponseBody
    Response  findIndexFiledList(
            @RequestParam(value = "is_all",defaultValue = "true") boolean is_all){
        try {
            return new Response().success(EsDataUtil.getIndexFiledList(client,"",is_all));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }

    /**
     * 批量删除
     * @param index 索引
     * @param body 参数
     * @return 数量
     */
    @PostMapping("{index}/batch")
    @ApiOperation(value = "批量删除数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：<br>" +
                    "1、ids 编号集合【List<String>】<br>" +
                    "2、uid 用户编号<br>" +
                    "",paramType = "body")
    })
    public @ResponseBody
    Response  deleteBatch(@PathVariable("index") String index,
                               @RequestBody Map<String,Object> body) {

        // 逻辑还是物理删除
        try {
            List<String> ids = ParamsUtil.checkMapList(body,"ids");
            String uid = ParamsUtil.getUid(body);
            return new Response().success(api.delete(client,index,ids,uid));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }




    /* *//**
     * 根据索引名称获取字段
     * @param index 索引名称
     * @param is_all 是否显示全部字段 true/false【分词】
     * @return 返回集合数据
     * @throws Exception 异常信息
     *//*
    @GetMapping("{index}/field")
    @ApiOperation(value = "根据索引名称获取字段集合")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index",value = "索引名称",required = true,paramType = "path"),
            @ApiImplicitParam(name = "is_all",value = "是否显示全部字段 true/false【分词】",defaultValue = "true",paramType = "query"),
    })
    public List<Map<String,Object>> findIndexFiledList(@PathVariable("index") String index,
                                                       @RequestParam(value = "is_all",defaultValue = "true") boolean is_all) throws Exception {
        return api.findIndexFiledList(client,index,is_all);
    }

    *//**
     * 分页列表
     * @param index 索引
     * @param body 参数
     * @return 返回分页对象
     * @throws Exception 异常信息
     *//*
    @PostMapping("{index}/page")
    @ApiOperation(value = "分页列表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、pageNo 当前页面【int】</br>" +
                    "2、pageSize 每页默认显示条数【int】</br>" +
                    "3、fields 字段显示或隐藏 {filed:int[1 显示 0隐藏]}</br>" +
                    "4、is_high 是否高亮显示 true/false</br>" +
                    "5、order 排序{filed:int[1升序-1降序]}</br>" +
                    "6、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "7、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "8、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "9、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "10、uid 当前登录账号信息 String",required = true,defaultValue = "{}")
    })
    public Map<String,Object> page(@PathVariable("index") String index,
                                   @RequestBody Map<String,Object> body) throws Exception {
        return api.page(client,index,body);
    }


    *//**
     * 列表
     * @param index 索引
     * @param body 参数
     * @return 返回分页对象
     * @throws Exception 异常信息
     *//*
    @PostMapping("{index}/list")
    @ApiOperation(value = "不分页列表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、fields 字段显示或隐藏 {filed:int[1 显示 0隐藏]}</br>" +
                    "2、is_high 是否高亮显示 true/false</br>" +
                    "3、order 排序{filed:int[1升序-1降序]}</br>" +
                    "4、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "5、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "6、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "7、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "10、uid 当前登录账号信息 String",required = true,defaultValue = "{}")
    })
    public List<Map> list(@PathVariable("index") String index,
                          @RequestBody Map<String,Object> body) throws Exception {
        return api.findList(client,index,body);
    }

    *//**
     * 新增或编辑
     * @param index 索引
     * @param body 参数
     * @return 信息或编辑的树编号
     * @throws Exception 异常信息
     *//*
    @PostMapping("{index}/saveOrUpdate")
    @ApiOperation(value = "新增或编辑")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：</br>" +
                    "1、saveOrUpdate {field:value[Object]}</br>" +
                    "2、uid 当前登录账号信息 String",required = true,defaultValue = "{}")
    })
    public String saveOrUpdate(@PathVariable("index") String index,
                               @RequestBody Map<String,Object> body) throws Exception {
        return api.saveOrUpdate(client,index,body);
    }

    *//**
     * 单个删除数据
     * @param index 索引
     * @param id 数据编号
     * @param uid 用户数据编号
     * @param is_remove 删除类型 true  物理删除 false 逻辑删除
     * @param is_refresh 是否刷新 true  刷新 false 不刷新
     * @return 返回对象数据
     * @throws Exception 异常信息
     *//*
    @DeleteMapping("{index}/{id}")
    @ApiOperation(value = "删除单条数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "id", value = "数据编号", dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "uid", value = "用户编号", dataType="string",defaultValue = "",paramType = "query"),
            @ApiImplicitParam(name = "is_remove", value = "是否物理删除 true/false", dataType="boolean",defaultValue = "false",paramType = "query"),
            @ApiImplicitParam(name = "is_refresh", value = "是否立即刷新 true/false", dataType="boolean",defaultValue = "false",paramType = "query")
    })
    public boolean delete(@PathVariable("index") String index,
                          @PathVariable("id") String id,
                          @RequestParam(value = "uid",defaultValue = "09901") String uid,
                          @RequestParam(value = "is_remove",defaultValue = "false") boolean is_remove,
                          @RequestParam(value = "is_refresh",defaultValue = "false") boolean is_refresh) throws Exception {
        return  api.delete(client,index,id,uid,is_remove,is_refresh);
    }

    *//**
     * 批量删除
     * @param index 索引
     * @param body 参数
     * @param uid 用户编号
     * @param is_remove 删除类型 true  物理删除 false 逻辑删除
     * @return 数量
     * @throws Exception 异常信息
     *//*
    @PostMapping("{index}/batch")
    @ApiOperation(value = "批量删除数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "uid", value = "用户数据编号", dataType="string",defaultValue = "",paramType = "path"),
            @ApiImplicitParam(name = "body", value = "body参数，参数如下：<br>" +
                    "1、ids 编号集合【List<String>】<br>" +
                    "",paramType = "body"),
            @ApiImplicitParam(name = "is_remove", value = "是否物理删除 true/false", dataType="boolean",defaultValue = "false",paramType = "query"),
            @ApiImplicitParam(name = "is_refresh", value = "是否立即刷新 true/false", dataType="boolean",defaultValue = "false",paramType = "query")
    })
    public long deleteBatch(@PathVariable("index") String index,
                            @RequestBody Map<String,Object> body,
                            @RequestParam(value = "uid",defaultValue = "09901") String uid,
                            @RequestParam(value = "is_remove",defaultValue = "false") boolean is_remove) throws Exception {
        List<String> ids = ParamsUtil.checkMapList(body,"ids");
        // 逻辑还是物理删除
        return  api.deleteBatch(client,index,ids,uid,is_remove);
    }

    *//**
     * 分组统计 【单个字段统计】
     * @param index 索引
     * @param field 字段
     * @param size 前nt条数据 -1 全部
     * @param groupCount 统计结果排序
     * @param map_body 参数
     * @return 返回统计结果
     * @throws Exception 异常信息
     *//*
    @PostMapping("{index}/group")
    @ApiOperation(value = "单个字段统计")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称",dataType="string",defaultValue = "",paramType = "path",required = true),
            @ApiImplicitParam(name = "field", value = "统计字段", dataType="string",defaultValue = "",paramType = "query",required = true),
            @ApiImplicitParam(name = "size", value = "前n条显示<br>-1时显示全部", dataType="int",defaultValue = "",paramType = "query"),
            @ApiImplicitParam(name = "groupCount", value = "统计数量排序", dataType="int",defaultValue = "-1",paramType = "query"),
            @ApiImplicitParam(name = "map_body", value = "body参数" +
                    "1、order 排序{filed:int[1升序-1降序]}</br>" +
                    "2、exact_search 精确查找 {field[字段名]:value[字段值 字符串]}<br>" +
                    "3、search 模糊查找 {field[字段名]:value[字段值 字符串]} <br>" +
                    "4、in_search in字句 {field[字段名]:value[字段值 List<T>]}<br>" +
                    "5、rang_search 区间查询 {field[字段名]:value[字段值 {start:\"\",end:\"\",format:\"number 数字，time 时间 date 日期 year 年 month 月\"}]}<br>" +
                    "6、uid 当前登录账号信息 String", paramType = "body"),
    })
    public List<Map<String,Object>> findCount(@PathVariable("index") String index,
                               @RequestParam(value = "field",required = true) String field,
                               @RequestParam(value = "size",defaultValue = "-1") int size,
                               @RequestParam(value = "groupCount",defaultValue = "-1") int groupCount,
                               @RequestBody Map<String,Object> map_body) throws Exception{
        return api.findCountByGroup(client,index,field,groupCount,size,map_body);
    }*/

    /**
     * 批量更新
     * @param index 索引名称
     * @param map 存储对象
     * @return 返回数据对象
     */
    @PostMapping("{index}/insertBatch")
    @ApiOperation(value = "批量更新")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index",value = "索引名称",required = true,paramType = "path"),
            @ApiImplicitParam(name = "map", value = "body参数",paramType = "body")
    })
    public @ResponseBody
    Response  insertBatch(@PathVariable("index") String index, @RequestBody Map<String,Object> map) {
        try {
            List<Map<String,Object>> save = ParamsUtil.checkListMapValue(map,"saves");
            return new Response().success(actApi.insertBatch(client,index,save));
        } catch (Exception e) {
            return new Response().error(e.getMessage());
        }
    }
}
