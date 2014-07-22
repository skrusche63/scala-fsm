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
import org.apache.hadoop.mapred.JobConf

import de.kp.core.spade.SpadeAlgorithm
import de.kp.core.spade.hadoop.io.{ExtEquivalenceClassListReader,PatternWriter}

class SpadeEngine (args: Args) extends Job(args) {

  override implicit val mode = new Hdfs(true, new Configuration())

  override def run: Boolean = {
    	
    val dfs = true
    val support = args("support").toDouble

    val algorithm = new SpadeAlgorithm(support, dfs)

    val keep = true;
    val verbose = false;

    val jobConf = new JobConf()
        
    /**
     * Read the list of equivalence classes that respect the
     * minimum support threshold for their assigned items from
     * the HDFS
     */
    val reader = new ExtEquivalenceClassListReader()        
    val eqClasesWritable = reader.read(args("input"), jobConf)
		
	val total = eqClasesWritable.getSequences()
	val eqClases = eqClasesWritable.getEqClases()

    algorithm.runAlgorithm(eqClases, total, keep, verbose)
        
    val patterns = algorithm.getResult()
 
    println("==============================")
    for (i <- 0 until patterns.size()) {
      println(patterns.get(i).toStringToFile())
    }
    println("==============================")
    
    println(algorithm.printStatistics())
    
    val writer = new PatternWriter(args("output") + "/pattern", jobConf)
    writer.write(patterns)
    
    true
    
  }
  
}