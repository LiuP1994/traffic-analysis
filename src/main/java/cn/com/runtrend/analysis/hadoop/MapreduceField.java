package cn.com.runtrend.analysis.hadoop;

import cn.com.runtrend.analysis.commons.Constants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: 张乔齐
 * @Description: mapreduce 字段操作工具类
 * @Date: 2017/8/8
 * @Modified By:
 */
public class MapreduceField {

  /**
   * 将传入的字段字符串转为 map 集合
   *
   * @param fields 字段字符串
   * @return map
   */
  private static Map<String, Integer> fields2Map(String fields) {
    String[] fieldArray = fields.split(",");
    Map<String, Integer> fieldMap = new HashMap<String, Integer>();

    for (int i = 0; i < fieldArray.length; i++) {
      if (i % 2 == 1) {
        fieldMap.put(fieldArray[i], Integer.parseInt(fieldArray[i - 1]));
      }
    }
    return fieldMap;
  }

  /**
   * 获取 mapreduce 字段属性
   *
   * @param value 字段值
   * @param field 字段名称
   * @return MrProperty
   * @see MrProperty
   */
  public static MrProperty getMrProperty(String value, String field) {
    Map<String, Integer> fieldMap = fields2Map(field);
    if (null == value || null == field || "".equals(field)) {
      return new MrProperty(fieldMap, Collections.<String>emptyList());
    }
    String[] values = value.split(Constants.FIELDS_TERMINATED, -1);
    if (values.length >= fieldMap.size()) {
      return new MrProperty(fieldMap, Arrays.asList(values));
    } else {
      return new MrProperty(fieldMap, Collections.<String>emptyList());
    }
  }

  /**
   * 列表转字符串
   *
   * @param fieldList 字段列表
   * @return string
   */
  public static String list2String(List<String> fieldList) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldList.size(); i++) {
      sb.append(fieldList.get(i));
      if (i < fieldList.size() - 1) {
        sb.append(Constants.FIELDS_TERMINATED);
      }
    }
    return sb.toString();
  }
}
