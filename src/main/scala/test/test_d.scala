package test

/**
  * scala的流文件
  */

import java.io
import java.io.{File, PrintWriter}

import scala.io.{Source, StdIn}
import scala.reflect.io.Path
object test_d {
  def main(args: Array[String]): Unit = {


    //可以直接指定输出路径
    val write = new PrintWriter(new File("C:\\Users\\asus\\Desktop\\一些文件\\test.txt"))
    write.write("素衣莫起风尘叹，犹及清明可到家" +"\n"+
      "江南好，风景旧曾谙")
    write.close()
  }
}

object test_e {

  def main(args: Array[String]): Unit = {
    println("输入陆游的一句诗歌：")
    val line = StdIn.readLine()
    println("很棒，你输入的是：" + line)
  }
}

object test_f {
  def main(args: Array[String]): Unit = {
    println("文件内容为：")

    //也可以直接指定输入路径
    Source.fromFile("test.txt").foreach(
      print
    )
  }
}
