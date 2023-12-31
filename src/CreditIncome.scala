import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.PrintWriter

object CreditIncome {
  def main(args: Array[String]) {
    val inputFile =  ".\\src\\application_data.csv"
    val conf = new SparkConf().setAppName("CreditCount").setMaster("local")
    val sc = new SparkContext(conf)
    val csv = sc.textFile(inputFile)
    val header = csv.first()
    val lines = csv.filter(_ != header)
    val format = lines.map(x => ((x.split(',')(0),x.split(',')(1),x.split(',')(7),x.split(',')(6)), x.split(',')(7).toDouble - x.split(',')(6).toDouble))
    val r_sorted = format.sortBy(_._2,true)
    val min = r_sorted.take(10)
    val d_sorted = format.sortBy(_._2,false)
    val max = d_sorted.take(10)

    val outFile = new PrintWriter(".\\CreditIncome.txt")
    min.foreach(outFile.println)
    max.foreach(outFile.println)
    outFile.close()
  }
}