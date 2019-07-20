package cn.com.runtrend.analysis.hadoop.idmapping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther: 张乔齐
 * @Description: ID图谱操作类
 * @Date: 2017/9/7
 * @Modified By:
 */
public class IdMapping {

  public void run(String[] args) {
    Configuration conf = new Configuration();
    try {
      String[] otherArgs = new GenericOptionsParser(
          conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
        System.err.println("idmapping <in> <fields>");
        System.exit(2);
      }
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(otherArgs[0] + "_id1"), true);
      fs.delete(new Path(otherArgs[0] + "_Id2Phone"), true);
      fs.delete(new Path(otherArgs[0] + "_Id2Uid"), true);
      fs.delete(new Path(otherArgs[0] + "_Id2Terminal"), true);
      boolean job1 = Id1.run(args);
      boolean job2 = false;
      if (job1) {
        job2 = Id2Phone.run(args);
      }
      boolean job3 = false;
      if (job2) {
        job3 = Id2Uid.run(args);
      }
      if (job3) {
        Id2Terminal.run(args);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    IdMapping job = new IdMapping();
    job.run(args);
  }
}
