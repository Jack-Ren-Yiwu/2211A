package util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Time : 2022/11/15 13:12
 * @Author :tzh
 * @File : ThreadPoolUtil
 * @Project : gmall_flink
 * 懒汉单例模式
 * 1.对象私有化
 * 2.构造器私有化
 * 3.提供公共的获取对象的方法
 **/
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;
    private ThreadPoolUtil() {}
    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
             /* ·corePool：核心线程池的大小。
            ·maximumPool：最大线程池的大小。
            ·BlockingQueue：用来暂时保存任务的工作队列。
            ·RejectedExecutionHandler：当ThreadPoolExecutor已经关闭或ThreadPoolExecutor已经饱和时
            （达到了最大线程池大小且工作队列已满），execute()方法将要调用的Handler。
              */
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                            16,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
