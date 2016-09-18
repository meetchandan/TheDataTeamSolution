import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object InventoryMatching extends App{
  private val sc = new SparkContext(new SparkConf())

  def solution() = {
    val recordsRdd = sc.textFile("/Users/Chandanb/data").map(x => mapToRecord(x))
    val sellRDD = recordsRdd.filter(x => x.transaction_type == "Sell")
    val sellRDDByProduct  = sellRDD.map(x => (x.product, x))
    val buyRDD = recordsRdd.filter(x => x.transaction_type == "Buy")
    val buyRDDByProduct = buyRDD.map(x => (x.product,x))
    val groupedRdd = sellRDDByProduct.groupWith(buyRDDByProduct)
    val netProfit = sc.accumulator(0.0)
    groupedRdd.foreach(l => {
      var buySorted = l._2._2.toList.sortBy(x=> x.transaction_date)
      var sellSorted = l._2._1.toList.sortBy(x=> x.transaction_date)
      var i = 0
      var j = 0
      for(i <- sellSorted ){
        var profit = 0.0
        var j = 0
        for(j <- buySorted){
          while(i.quantity > 0 && j.quantity > 0){
            if(j.quantity >= i.quantity) {
              profit += i.quantity*i.price - i.quantity*j.price
              j.quantity = j.quantity - i.quantity
              i.quantity = 0
            }
            else{
              profit += j.quantity*i.price - j.quantity*j.price
              i.quantity = i.quantity - j.quantity
              j.quantity = 0
            }
          }
        }
        println(profit)
        netProfit.add(profit)
        profit = 0.0
      }
    })
    println("Net Profit:" + netProfit)
  }

  def mapToRecord(s: String) = {
    val values = s.split(",")
    new Record(values(0).trim.toInt, values(1).trim, values(2).trim.toInt, values(3).trim.toDouble, values(4).trim.toInt)
  }

  class Record(var product: Int, var transaction_type: String, var quantity: Int, var price: Double, var transaction_date: Int) extends Serializable

}