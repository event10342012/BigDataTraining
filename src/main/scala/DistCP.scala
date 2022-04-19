import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer
import scala.sys.exit
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec


case class SparkDistCPOptions(ignoreFailures: Boolean, maxConcurrence: Int)


object DistCP {
  def makeDir(spark: SparkSession, sourcePath: Path, targetPath: Path,
              fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(sourcePath).foreach(curPath => {
      if (curPath.isDirectory) {
        val subPath = curPath.getPath.toString.split(sourcePath.toString)(1)
        val nextTargetPath = new Path(targetPath + subPath)
        try {
          fs.mkdirs(nextTargetPath)
        } catch {
          case ex: Exception => if (!options.ignoreFailures) throw ex else println(ex.getMessage)
        }
        makeDir(spark, curPath.getPath, nextTargetPath, fileList, options)
      } else {
        fileList.append((curPath.getPath, targetPath))
      }
    })
  }

  def copy(spark: SparkSession, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
    val sc = spark.sparkContext
    val maxConcurrenceTasks = Some(options.maxConcurrence).getOrElse(5)
    val rdd = sc.makeRDD(fileList.toSeq, maxConcurrenceTasks)

    rdd.mapPartitions(iter => {
      val hadoopConf = new Configuration()
      iter.foreach(tup => {
        try {
          FileUtil.copy(tup._1.getFileSystem(hadoopConf), tup._1, tup._2.getFileSystem(hadoopConf), tup._2,
            false, hadoopConf)
        } catch {
          case ex: Exception => if (!options.ignoreFailures) throw ex else println(ex.getMessage)
        }
      })
      iter
    }).collect()
  }

  def main(args: Array[String]): Unit = {
    def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      list match {
        case Nil => map
        case "-i" :: value :: tail =>
          nextArg(map ++ Map("i" -> value.toBoolean), tail)
        case "-m" :: value :: tail =>
          nextArg(map ++ Map("m" -> value.toInt), tail)
        case unknown :: _ =>
          println("Unknown option " + unknown)
          exit(1)
      }
    }

    val options = nextArg(Map(), args.toList)

    val fileList = new ArrayBuffer[(Path, Path)]()
    val sourcePath: Path = new Path("/Users/leochen/PycharmProjects/mdp")
    val targetPath: Path = new Path("/Users/leochen/hw")
    val sparkDistCPOptions = SparkDistCPOptions(options.getOrElse("i", true).asInstanceOf,
      options.getOrElse("m", 5).asInstanceOf)

    val spark = SparkSession
      .builder
      .appName("DistCP")
      .getOrCreate()
  }
}
