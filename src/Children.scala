import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.PrintWriter

object Children {
  def main(args: Array[String]) {
    val inputFile =  ".\\src\\application_data.csv"
    val conf = new SparkConf().setAppName("CreditCount").setMaster("local")
    val sc = new SparkContext(conf)
    val csv = sc.textFile(inputFile)
    val header = csv.first()
    val lines = csv.filter(_ != header)
    val males = lines.filter(_.split(',')(2) == "M")
    val child_num = males.map(x => (x.split(",")(5).toInt, 1)).reduceByKey((x,y) => x+y)
    val sum = males.count()
    val child_rate = child_num.mapValues(x => x.toDouble/sum.toDouble).sortByKey()
    child_rate.saveAsTextFile(".\\Children out")
  }
}