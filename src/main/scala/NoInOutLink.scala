import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

import org.apache.log4j.{Level, Logger} //this is simply used to eliminate unnecesary info from the console



object NoInOutLink {
    def main(args: Array[String]) {
		val MASTER_ADDRESS = "ec2-35-160-94-93.us-west-2.compute.amazonaws.com"
		val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
		val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
		val INPUT_DIR = HDFS_MASTER + "/pagerank/input"

        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
			.setMaster(SPARK_MASTER)
            .setAppName("NoInOutLink")

        val sc = new SparkContext(conf)
		
		//this is simply used to eliminate unnecesary info from the console
		val rootLogger = Logger
			.getRootLogger()
			.setLevel(Level.ERROR)
		
        val links = sc
            .textFile(links_file, num_partitions)
            
		//split the links into two by :
		val splits = links
			.map( line => line.split(":") )
		
		val outlinks = splits
			.map(arr => (arr(0).toInt, arr(0))) //those that have outlinks
		
		val inlinks = splits
			.map(arr => arr(1))
			.flatMap( line => line.split(" ") )
			.filter( word => word != "" )
			.distinct()
			.map(x => (x.toInt, x)) //those that have inlinks
		
		//zip link titles with indices, where indices have values from 1, 2...
        val titles = sc
            .textFile(titles_file, num_partitions)
			.zipWithIndex.map{case (v,k) => (k.toInt+1, v)}
            

        /* No Outlinks */
		println("[ NO OUTLINKS ]")
        val no_outlinks = titles.subtractByKey(outlinks) //by subtraction obtain those that have no outlinks
			.takeOrdered(10)							 //take ordered 10 by index, where order is ascending by default
			.foreach(println)
        
        

        /* No Inlinks */
		println("\n[ NO INLINKS ]")
        val no_inlinks = titles.subtractByKey(inlinks)  //by subtraction obtain those that have no outlinks
			.takeOrdered(10)							//take ordered 10 by index, where order is ascending by default
			.foreach(println)

        
    }
}
