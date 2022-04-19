import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)

    val conf = new SparkConf().setAppName("InvertedIndex").setMaster("local")
    val sc = new SparkContext(conf)

    val distFile = sc.wholeTextFiles(inputPath)
    val group = distFile.
      map(x => (x._1.split("/").last, x._2.split("\r\n"))).
      flatMap {
        case (index, line) => line.flatMap(x => x.split(" ")).map { word => (word, index) }
      }.groupByKey()

    val result = group.map {
      case (word, indexes) =>
        val indexCountMap = scala.collection.mutable.HashMap[String, Int]()
        for (index <- indexes) {
          val count = indexCountMap.getOrElseUpdate(index, 0) + 1
          indexCountMap.put(index, count)
        }
        (word, indexCountMap)
    }.sortByKey().
      map(word => s"${word._1}:${word._2}")

    result.repartition(1).saveAsTextFile(outputPath)
  }
}
