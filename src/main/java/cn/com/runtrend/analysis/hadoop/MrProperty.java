package cn.com.runtrend.analysis.hadoop;

import java.util.List;
import java.util.Map;

/**
 * @Auther: 张乔齐
 * @Description: mapreduce 字段属性
 * @Date: 2017/8/9
 * @Modified By:
 */
public class MrProperty {

  private Map<String, Integer> fieldMap;
  private List<String> row;

  public MrProperty(Map<String, Integer> fieldMap, List<String> row) {
    this.fieldMap = fieldMap;
    this.row = row;
  }

  public Map<String, Integer> getFieldMap() {
    return fieldMap;
  }

  public void setFieldMap(Map<String, Integer> fieldMap) {
    this.fieldMap = fieldMap;
  }

  public List<String> getRow() {
    return row;
  }

  public void setRow(List<String> row) {
    this.row = row;
  }
}
