import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.PrintWriter

object AvgIncome {
  def main(args: Array[String]) {
    val inputFile =  ".\\src\\application_data.csv"
    val conf = new SparkConf().setAppName("CreditCount").setMaster("local")
    val sc = new SparkContext(conf)
    val csv = sc.textFile(inputFile)
    val header = csv.first()
    val lines = csv.filter(_ != header)
    val filtered_lines = lines.filter(x => x.split(",")(6).toDouble / -x.split(",")(13).toDouble > 1)
    val format = filtered_lines.map(x => (x.split(",")(0), x.split(",")(6).toDouble / -x.split(",")(13).toDouble))
    val sorted = format.sortBy(_._2,false)
    sorted.saveAsTextFile(".\\AvgIncome out")
  }
}