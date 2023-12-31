import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object CreditCount {
  def main(args: Array[String]) {
    val inputFile =  ".\\src\\application_data.csv"
    val conf = new SparkConf().setAppName("CreditCount").setMaster("local")
    val sc = new SparkContext(conf)
    val csv = sc.textFile(inputFile)
    val header = csv.first()
    val lines = csv.filter(_ != header)
    val credit = lines.map(_.split(',')(7).toDouble.toInt/10000)
    val group = credit.map(x => ((x.toString+"0000",(x+1).toString+"0000"), 1))
    val result = group.reduceByKey((x,y) => x+y)
    result.saveAsTextFile(".\\CreditCount out")
  }
}


