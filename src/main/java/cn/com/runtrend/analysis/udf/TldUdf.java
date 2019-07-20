package cn.com.runtrend.analysis.udf;

import cn.com.runtrend.analysis.commons.Tld;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Auther: 张乔齐
 * @Description:
 * @Date: 2017/11/6
 * @Modified By:
 */
public class TldUdf extends UDF {

  public String evaluate(String host) {
    List<String> hosts = Tld.getTlds(host);
    if (hosts.size() > 0) {
      return hosts.get(hosts.size() - 1);
    } else {
      return "unknow";
    }
  }
}
