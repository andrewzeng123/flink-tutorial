package com.cuimei
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object StreamingJob {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val text = env.socketTextStream("hadoop102",9999,'\n')

        import org.apache.flink.streaming.api.scala._
      /*  val windowCount = text.flatMap(w => w.split("\\s"))
                .map(w => WindowCount(w,1))
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")*/

        val windowCounts = text
                .flatMap { w => w.split("\\s") }
                .map { w => WindowWithCount(w, 1) }
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")

        windowCounts.print().setParallelism(1)

        env.execute("Socket Window WordCount")

    }
    //样例类
    case class WindowWithCount(word:String,count: Long)
    //不好
    //你好
    //时好时坏
    //今天不错
}
