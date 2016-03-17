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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author weigend
 */
public class Mapper implements Serializable {
    public String mapper(Object o) {
        try {            
            long sum = 0;
            for (long i = 0; i < 10000000; i++) {
                sum += Math.random() * 100;
            }
            return InetAddress.getLocalHost().toString() + " (" +  sum + ")";
        } catch (UnknownHostException ex) {
            Logger.getLogger(SparkClient.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }
}
