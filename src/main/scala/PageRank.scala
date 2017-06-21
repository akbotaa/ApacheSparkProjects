import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

import org.apache.log4j.{Level, Logger} //this is simply used to eliminate unnecesary info from the console

object PageRank {
    def main(args: Array[String]) {
		val MASTER_ADDRESS = "ec2-35-160-94-93.us-west-2.compute.amazonaws.com"
		val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
		val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
		val INPUT_DIR = HDFS_MASTER + "/pagerank/input"
		
        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
			.setMaster(SPARK_MASTER)
            .setAppName("PageRank")

        val sc = new SparkContext(conf)

		val rootLogger = Logger  //this is simply used to eliminate unnecesary info from the console
			.getRootLogger()
			.setLevel(Level.ERROR)

        val links = sc
            .textFile(links_file, num_partitions)
            
		//array of pairs, where each pair corresponds to (link, array of outlinks)
		val splits = links
			.map( line => line.split(":") )
			.map(x => (x(0).toInt, x(1)))
			.mapValues(x => x.split(" ").filter( word => word != "" ))
					
		//array of pairs, where each pair corresponds to (index, title) of a link.. (indices start from 1)
        val titles = sc
            .textFile(titles_file, num_partitions)
			.zipWithIndex
			.map{case (v,k) => (k.toInt+1, v)}
		
		//total number of links	
		val N = titles
			.count
			.toDouble
		
		//damping factor
		val d = 0.85
			
		//initialize PR_0
		//array of pairs, where each pair corresponds to (link, current PR of link)
		var pageRanks = titles.mapValues(x => 100/N)
		
		
		
        /* PageRank */
        for (i <- 1 to iters) {
			
			//array of pairs, where each pair corresponds to (link x, PR_{i-1}(y)/out(y) for some y->x) )..  (x includes only links with inlinks)
			val pairs = splits
				.join(pageRanks) 												//array of triples: (y, (array of outlinks of y, PR_{i-1}(y) ) )
				.values															//array of pairs: (array of outlinks of y, PR_{i-1}(y) ) 
				.map{case (to_list, rank) => (to_list, rank/to_list.length )}	//array of pairs: (array of outlinks of y, PR_{i-1}(y)/out(y) ) 
				.map{case (v,k) => (k,v)}										//array of pairs: (PR_{i-1}(y)/out(y), array of outlinks of y ) 
				.flatMapValues(x => x)											//array of pairs: (PR_{i-1}(y)/out(y), outlink of y )
				.map{case (v,k) => (k.toInt,v)}									//array of pairs: (outlink of y, PR_{i-1}(y)/out(y))
			
			//array of pairs (link x, PR_{i}(x) ) for all links x that have inlinks
			val inlinkPR = pairs
				.reduceByKey((x, y) => x + y)									//array of pairs: (link x, sum_{over all y->x} PR_{i-1}(y)/out(y)) )
				.mapValues(x => 100*(1-d)/N + x*d) 								//array of pairs: (link x, PR_{i}(x) )	
        		
			//update PR for links that have no inlink (done only once, since PR does not change after that)
			if (i==1){
				pageRanks = pageRanks
					.mapValues(x => x*(1-d))
			}
			
			//PR for links that have no inlink
			val noInlinkPR = pageRanks
				.subtractByKey(inlinkPR)
				
			//updated pageRanks: array of pairs (link, PR_i)
			pageRanks = noInlinkPR
				.union(inlinkPR)
			
			println(i)
		
		}

		//normalization factor
		val z = pageRanks.values.sum
		
		//normalized PR after iters # of iterations
		val normedPR = pageRanks.mapValues(x => x/z*100)
		
		// triple of (index, title, pr)
		val output = titles
			.join(normedPR)
			.map{case (ind, (title, pr)) => (ind, title, pr)}
			

		//print links with highest PR in descending order
        println("[ PageRanks ]")
        output.takeOrdered(10)(Ordering[Double].reverse.on(x => x._3))
			.foreach(println)
    }
}
