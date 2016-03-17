/*
 _____________________________________________________________________________
 
            Project:    SolrSpark
  _____________________________________________________________________________
  
         Created by:    Johannes Weigend, QAware GmbH
      Creation date:    2015 - 2016
  _____________________________________________________________________________
  
          License:      Apache License 2.0
  _____________________________________________________________________________ 

 */
package org.netbeans.spark;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTester {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTester");
        conf.setMaster("spark://192.168.1.100:7077,192.168.1.101:7077,192.168.1.102:7077");
        conf.set("spark.ui.enabled", "false");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("./build/libs/SparkTester.jar");
        List<Integer> tasks = new ArrayList<>();
        IntStream.range(0, 16).forEach((int x) -> tasks.add(x));
    
        // Spark - remote
        JavaRDD<Integer> parallelStream = sc.parallelize(tasks, 16);
        String result = parallelStream
                .map(x -> "host: " + InetAddress.getLocalHost() + ", ")
                .reduce((x, y) -> x + y);
        System.out.println(result);
        
        // Java 8 - local
        Optional<String> result2 = IntStream.range(0, 16).parallel()
                .mapToObj(x -> "tid: " + Thread.currentThread().getId() + ", ")
                .reduce((x, y) -> x + y);      
        System.out.println(result2);
    
    }
}
