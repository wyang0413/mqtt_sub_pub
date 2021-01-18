package utils;

import com.alibaba.fastjson.JSONObject;

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2020/7/16
 * Time:10:22
 * @Description:
 */
public class ParseJson {
    /**
     * 解析json数据
     *
     * @param data
     * @return
     */
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 判断字符串是不是json格式
     *
     * @param content
     * @return
     */
    public static boolean isJson(String content) {
        try {
            // 如果可以成功解析，则字符串为json格式
            JSONObject jsonStr = JSONObject.parseObject(content);
            return true;
        } catch (Exception e) {
            // 抛出异常则不是json
            return false;
        }

    }

    public static String toJson(Object o) {
        String json = JSONObject.toJSON(o).toString();
        return json;
    }
}