package cn.com.runtrend.analysis.commons;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Test {
  public static void main(String[] args) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(50, 50, 200, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(50));

    for(int i=0;i<15000;i++){
      int flag = 0;
      while (flag == 0) {
//        System.out.println(executor.getActiveCount());
        if(executor.getActiveCount() < 50) {
          MyTask myTask = new MyTask(i);
          executor.execute(myTask);
          flag = 1;
        }
      }
      System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
          executor.getQueue().size()+"，已执行玩别的任务数目："+executor.getCompletedTaskCount());
    }
    int flag = 0;
    while (flag == 0) {
      if(executor.getActiveCount() == 0) {
        executor.shutdown();
        flag = 1;
      }
    }

  }
}


class MyTask implements Runnable {
  private int taskNum;

  public MyTask(int num) {
    this.taskNum = num;
  }

  @Override
  public void run() {
    System.out.println("正在执行task "+taskNum);
    try {
      Thread.currentThread().sleep(60);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("task "+taskNum+"执行完毕");
  }
}
