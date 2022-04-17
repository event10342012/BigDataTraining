import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


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
    val fileList = new ArrayBuffer[(Path, Path)]()
    val sourcePath: Path = new Path(args.apply(0))
    val targetPath: Path = new Path(args.apply(1))
    val options = SparkDistCPOptions(true, 5)

    val spark = SparkSession
      .builder
      .appName("DistCP")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    makeDir(spark, sourcePath, targetPath, fileList, options)
    copy(spark, fileList, options)
  }
}
