package cn.com.runtrend.analysis.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Auther: 张乔齐
 * @Description: 用于解析 properties 文件
 * @Date: 2017/8/8
 * @Modified By:
 */
public class PropertiesParser {

  private static final String CONFIGURE = "/cnf.properties";

  /**
   * 根据关键字获取配置信息
   *
   * @param key 关键字
   * @return 配置信息
   */
  public static String getValue(String key) {
    Properties properties = new Properties();
    InputStream inputStream = PropertiesParser.class.getResourceAsStream(CONFIGURE);
    try {
      properties.load(inputStream);
      return properties.getProperty(key);
    } catch (IOException e) {
      return "";
    }
  }
}
