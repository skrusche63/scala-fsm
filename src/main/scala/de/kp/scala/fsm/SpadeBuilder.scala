package de.kp.scala.fsm
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Scala-FSM project
* (https://github.com/skrusche63/scala-fsm).
* 
* Scala-FSM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Scala-FSM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Scala-FSM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import com.twitter.scalding.{Job,Args}
import com.twitter.scalding._

import org.apache.hadoop.conf.Configuration

/**
 * This is a helper class to convert different input formats into
 * the common SPMF text format to describe a sequence database; 
 * 
 * note, that the raw SPMF format is extended by line numbers
 */
class SpadeBuilder(args:Args) extends Job(args) {

  override implicit val mode = new Hdfs(true, new Configuration())

  override def next: Option[Job] = {
    
    val input = args("output") + "/builder"
    val nextArgs = args + ("input", Some(input))
    
    Some(new SpadeExtractor(nextArgs))
    
  }

  val format = args("format")	  
  format match {
        
    case "BMS" => fromBMS()
        
    case "CSV" => fromCSV()
       
    case "KOSARAK" => fromKosarak()
        
    case "SNAKE" => fromSnake()

    case "SPMF" => fromSpmf()

    case _ => {}
      
  }

  def fromBMS() {
    
    val input = TextLine(args("input")).read      
      .mapTo('line -> ('uid, 'pid)) {

        line:String => {
          
          val parts = line.split("\t")  
          
          val uid = Integer.parseInt(parts(0).trim())
          val pid = Integer.parseInt(parts(1).trim())
       
          (uid,pid)
          
        }
      
      }.groupBy('uid) {
    	/**
    	 * All items that are associated with a certain uid are aggregated
    	 * into a single line
    	 */
        val seq:String = ""
        _.foldLeft(('uid,'pid) -> 'sequence)(seq) {
          (sequence:String, tuple:(Int,Int)) => {
            (sequence + tuple._2 + " -1 ")
            }
          }
        
      }.mapTo('sequence -> 'sequence) {
        /**
         * The sequence line is extended to indicate the end of a sequence
         * in SPMD format (-2)
         */
        sequence:String => 
          (sequence + "-2")
      
      }.groupAll {
        /**
         * This mechanism is used to assign line no to each sequence
         */
        val lno = 0          
        _.scanLeft('sequence -> 'lno)(lno) {
          (lno:Int, sequence:String) => lno + 1
         }
        
      }.mapTo(('sequence,'lno) -> 'line) {
        tuple:(String,Int) => {
          
          val head = "# Build by Scalding 0.10.0, " + System.currentTimeMillis()
          if (tuple._2 > 0) ("" + tuple._2 + "," + tuple._1) else head
          
        }
      }.write(TextLine(args("output") + "/builder"))
      
  }

  def fromCSV() {
    
    val input = TextLine(args("input")).read      
      .mapTo('line -> 'sequence) {

        line:String => {
          
          val sb = new StringBuffer()
          val parts = line.split(",")  
          
          for (i <- 0 until parts.length) {
            
            val item = Integer.parseInt(parts(i))
            sb.append(item + " -1 ")
          }
          
          sb.append("-2")
          sb.toString
        
        }
     
    }.groupAll {
        /**
         * This mechanism is used to assign line no to each sequence
         */
        val lno = 0          
        _.scanLeft('sequence -> 'lno)(lno) {
          (lno:Int, sequence:String) => lno + 1
         }
        
      }.mapTo(('sequence,'lno) -> 'line) {
        tuple:(String,Int) => {
          
          val head = "# Build by Scalding 0.10.0, " + System.currentTimeMillis()
          if (tuple._2 > 0) ("" + tuple._2 + "," + tuple._1) else head
          
        }
        
      }.write(TextLine(args("output") + "/builder"))
  
  }

  def fromKosarak() {
        
    val input = TextLine(args("input")).read      
      .mapTo('line -> 'sequence) {

        line:String => {
          
          val sb = new StringBuffer()
          val parts = line.split(" ")  
          
          for (i <- 0 until parts.length) {
            
            val item = Integer.parseInt(parts(i))
            sb.append(item + " -1 ")
          }
          
          sb.append("-2")
          sb.toString
        
        }

    }.groupAll {
        /**
         * This mechanism is used to assign line no to each sequence
         */
        val lno = 0          
        _.scanLeft('sequence -> 'lno)(lno) {
          (lno:Int, sequence:String) => lno + 1
         }
        
      }.mapTo(('sequence,'lno) -> 'line) {
        tuple:(String,Int) => {
          
          val head = "# Build by Scalding 0.10.0, " + System.currentTimeMillis()
          if (tuple._2 > 0) ("" + tuple._2 + "," + tuple._1) else head
          
        }
        
      }.write(TextLine(args("output") + "/builder"))
	
  }
	
  def fromSnake() {
  
    val input = TextLine(args("input")).read      
      .mapTo('line -> 'sequence) {

        line:String => {
		  /**
		   * If the line contains more than 11 elements, we use this 
		   * to filter smaller lines
		   */
          val len = line.length()
          if (len >= 11) {

            val sb = new StringBuffer()
            /** 
             * for each integer on this line, we consider that it is an item 
             */
		    for (i <- 0 until len) {
						
		      /** 
		       * We subtract 65 to get the item number and 
		       * write the item to the file
		       */
			  val character = line.toCharArray()(i) - 65
			  sb.append(character + " -1 ")
			
		    }
			
            sb.append("-2")        
            sb.toString

          }
          
        }
        
     }.groupAll {
        /**
         * This mechanism is used to assign line no to each sequence
         */
        val lno = 0          
        _.scanLeft('sequence -> 'lno)(lno) {
          (lno:Int, sequence:String) => lno + 1
         }
        
      }.mapTo(('sequence,'lno) -> 'line) {
        tuple:(String,Int) => {
          
          val head = "# Build by Scalding 0.10.0, " + System.currentTimeMillis()
          if (tuple._2 > 0) ("" + tuple._2 + "," + tuple._1) else head
          
        }
      
      }.write(TextLine(args("output") + "/builder"))

  }

   def fromSpmf() {
    
    val input = TextLine(args("input")).read      
      .groupAll {
        /**
         * This mechanism is used to assign line no to each sequence
         */
        val lno = 0          
        _.scanLeft('line -> 'lno)(lno) {
          (lno:Int, sequence:String) => lno + 1
         }
        
      }.mapTo(('line,'lno) -> 'line) {
        tuple:(String,Int) => {
          
          val head = "# Build by Scalding 0.10.0, " + System.currentTimeMillis()
          if (tuple._2 > 0) ("" + tuple._2 + "," + tuple._1) else head
          
        }
        
      }.write(TextLine(args("output") + "/builder"))
  
  }

}
