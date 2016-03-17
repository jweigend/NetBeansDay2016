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

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkClient {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println("Hallo Spark from Java!");

        final String APP_NAME = "SparkClient";
        final String MASTER = "spark://192.168.1.100:7077,192.168.1.101:7077,192.168.1.102:7077"; // "local[4]";

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("./build/libs/NetBeansGradleScala.jar");

        List tasks = new ArrayList();
        for (int i = 0; i < 16; i++) {
            tasks.add("Task: " + i);
        }
        //Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8");
        
        JavaRDD<String> parallelTasks = sc.parallelize(tasks, 16);
              
        Mapper m = new Mapper();
        String result = parallelTasks
                .map(m::mapper)
                .reduce((a, b) -> a + "," + b);
        
        System.out.println(result);
    }
}
