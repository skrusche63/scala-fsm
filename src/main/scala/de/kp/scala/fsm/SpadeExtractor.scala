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
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration

import de.kp.core.spade.{AbstractionQualitative,EquivalenceClass,Item,ItemAbstractionPair,Itemset,Sequence}
import de.kp.core.spade.{IDListBitmap,Pattern}

import de.kp.core.spade.hadoop.ExtEquivalenceClassListWritable

import scala.collection.mutable.{ArrayBuffer,HashMap}

/**
 * A temporary data structure the sequence database is converted to
 */
case class ItemEquivalenceClassMap(
  size:Int,
  items:HashMap[Item[java.lang.Integer],EquivalenceClass]
)

class SpadeExtractor(args: Args) extends Job(args) {

  override implicit val mode = new Hdfs(true, new Configuration())

  override def next: Option[Job] = {
    
    val input = args("output") + "/freq-eqc"
    val nextArgs = args + ("input", Some(input))
    
    Some(new SpadeEngine(nextArgs))
    
  }

  val support = args("support").toDouble
  
  /**
   * This RichPipe converts a textual representation of a sequence database
   * into a list of Sequences
   */
  val sequences = TextLine(args("input")).read   
    /**
     * A file containing sequences may have user defined comments,
     * starting with '#'
     */
    .filter('line) {line:String => (line.charAt(0) != '#')}
    /**
     * The textual representation of a sequence is transformed into
     * a binary one
     */
    .mapTo('line -> 'sequence) {line:String => createSequence(line)}
  
  /**
   * The sequences are transformed into a map[item,eqclas]
   */
  val itemEqClases = sequences.groupAll {
    
    val initItemEqClases = new ItemEquivalenceClassMap(0, HashMap.empty[Item[java.lang.Integer],EquivalenceClass])
    
    _.foldLeft[ItemEquivalenceClassMap,Sequence]('sequence -> 'itemEqClases)(initItemEqClases) {
     (itemEqClases:ItemEquivalenceClassMap,sequence:Sequence) => {
       
       val size = itemEqClases.size + 1
       val items = itemEqClases.items
       
       val sid = sequence.getId()
      
       val itemsets = sequence.getItemsets()
       for (i <- 0 until itemsets.size()) {
         
         val itemset = itemsets.get(i);
         val timestamp = itemset.getTimestamp()
         
         for (j <- 0 until itemset.size()) {
           
           val item = itemset.get(j).asInstanceOf[Item[java.lang.Integer]]
           items.get(item) match {
             
             case None => {

               val idList = new IDListBitmap()
               idList.registerBit(sid,timestamp.toInt)
            
               /**
                * See AbstractionCreator_Qualitative 
                */
               val abstraction = AbstractionQualitative.create(false)
            
               val pair:ItemAbstractionPair = new ItemAbstractionPair(item, abstraction)
            
               val pattern = createPattern(pair);
               val equivClas = new EquivalenceClass(pattern, idList)
               
               items += item -> equivClas
                
             }
             
             case Some(equivClas) => {
               
               val idList = equivClas.getIdList().asInstanceOf[IDListBitmap]
               idList.registerBit(sid,timestamp.toInt)

             }
           
           }
         
         }
       
       }
                   
       new ItemEquivalenceClassMap(size,items)
     
     }
       
    }
        
  }
  
  /**
   * Determine list of EqClases that respect the minimun threshold
   */
  val freqEqClases = itemEqClases.insert('support,support).mapTo(('itemEqClases,'support) -> 'freqEqClases) {
    /**
     * This is a mechanism to evaluate the ItemEqClases determined so far, and
     * transforming them into a list of equivalence classes that respect the 
     * minimum support threshold
     */
    tuple:(ItemEquivalenceClassMap,Double) => {
      
      val supp = tuple._2
      val itemEqClases = tuple._1
     
      val minsupp = Math.ceil(supp * itemEqClases.size)      
      val eqClasBuffer = ArrayBuffer.empty[EquivalenceClass]
      
      itemEqClases.items.foreach(entry => {
        
        val equivClas = entry._2
        if (equivClas.getIdList().getSupport() >= minsupp) {
          equivClas.getIdList().setAppearingSequences(equivClas.getClassIdentifier())
          
          eqClasBuffer += equivClas
          
        }
        
      })
      
      new ExtEquivalenceClassListWritable(itemEqClases.size, minsupp, eqClasBuffer.toList)
    
    }
  
  }
  
  val output = new SequenceFile(args("output") + "/freq-eqc" , 'freqEqClases)
  freqEqClases.write(output)

  val freqItems = itemEqClases.mapTo('itemEqClases -> 'freqItems) {
    
    /**
     * This is a mechanism to evaluate the ItemEqClases determined so far, and
     * transforming them into a list of items that respect the minimum support 
     * threshold
     */
    itemEqClases:ItemEquivalenceClassMap => {
      
      val minsupp = Math.ceil(support * itemEqClases.size)      
      val freqItemsBuffer = ArrayBuffer.empty[Item[java.lang.Integer]]
      
      itemEqClases.items.foreach(entry => {
        
        val equivClas = entry._2
        if (equivClas.getIdList().getSupport() >= minsupp) {
          freqItemsBuffer += entry._1          
        }
        
      })
      
      freqItemsBuffer.toList
      
    }
  }

  /**
   * Reduce the sequence database to those items that are
   * frequent, i.e. above the min support threshold
   */
  val freqSequences = sequences.crossWithTiny(freqItems)
    .filter(('sequence, 'freqItems)) {
      tuple:(Sequence,List[Item[java.lang.Integer]]) => {
      
        val sequence = tuple._1
        val fItems   = tuple._2
      
        val clone = sequence.cloneSequence()
      
        for (i <- 0 until clone.size()) {

          val itemset = clone.get(i)
          for (j <- 0 until itemset.size()) {
          
            val item = itemset.get(j)
            if (fItems.contains(item) == false) sequence.get(i).removeItem(j)
            
          }
        
          if (clone.get(i).size() == 0) sequence.remove(i)
        
        }
      
        (sequence.size() > 0)
      
    }
  
  }.groupAll(_.size).write(new SequenceFile(args("output") + "/freq-seq"))
  
  /**
   * A private helper method to retrieve the sequence representation
   * from the textual format of a sequence description
   */
  private def createSequence(line:String):Sequence = {
            
    val parts = line.split(",");
        
    val sid:Int = parts(0).toInt
    val ids:Array[String] = parts(1).split(" ")

    var timestamp:Long = -1

    var itemset:Itemset = new Itemset()
    val sequence:Sequence = new Sequence(sid)
        
    for (i <- 0 until ids.length) {
          
      val id = ids(i)
      if (id.codePointAt(0) == '<') {

        /**
         * Process timestamp
         */
        val value = id.substring(1, id.length - 1)
                
        timestamp = value.toLong
        itemset.setTimestamp(timestamp)
            
      } else if (id.equals("-1")) {
        /**
         * Process end of itemset: add itemset to 
         * sequence and initialize itemset for next
         */
        val time = itemset.getTimestamp() + 1
        sequence.addItemset(itemset)
            
        itemset = new Itemset()
        itemset.setTimestamp(time)
            
            
        timestamp += 1
            
      } else if (id.equals("-2")) {
        /** 
         *  End of sequence, do nothing
         */

      } else {
        /**
         * Process item
         */
        val itemid:java.lang.Integer = id.toInt
        
        val item = new Item(itemid)
        itemset.addItem(item)
            
        if (timestamp < 0) {
              
          timestamp = 1
          itemset.setTimestamp(timestamp)
                
        }
          
      }
        
    }

    sequence
    
  }
  
  private def createPattern(pair:ItemAbstractionPair):Pattern = {
    
    val elements = ArrayBuffer.empty[ItemAbstractionPair]
    elements += pair
    
    new Pattern(elements.toList)

  }
}