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

import com.twitter.scalding.Args
/**
 * SpadeApp is an example of how to use Builder, Extractor and Engine
 * in a chain of Scalding jobs
 */
object SpadeApp {

  def main(args:Array[String]) {

    val input  = "/Work/tmp/spmf/BMS1_spmf.txt"
	val output = "/Work/tmp/spmf/output"

    val params = List(
        "--input",input, 
        "--output",output,
        "--format", "SPMF",
        "--support", "0.00085"
        )
    
    val args = Args(params)
    
    val start = System.currentTimeMillis()
    
    /**
     * Build sequence database from custom data
     */    
    val builder = new SpadeBuilder(args)    
    builder.run
    
    println("Extractor started...")
    
    builder.next match {
      
      case None => {}     
      case Some(extractor) => {
        /**
         * Extract frequent equivalence classes from sequence database
         */
        extractor.run
        
        extractor.next match {
          
          case None => {}
          case Some(engine) => {
    
            println("Engine started...")
            engine.run  
            
            val end = System.currentTimeMillis()           
            println("Total time: " + (end-start) + " ms")
            
          }
          
        }
        
      }
	
    }
    
  }
}