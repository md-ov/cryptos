package com.minhdd.cryptos.scryptosbt.service.segment

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.scalatest.FunSuite

class SplitterTest extends FunSuite {
    
    val beforeSplit1 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:00:00"), 10535.2D)
    val beforeSplit2 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:15:00"), 20535.2D)
    val beforeSplit3 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:30:00"), 535.2D)
    
    
    test("test toSmallSegments 1 element") {
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit1))
        
        assert(splits.size == 1)
        assert(splits.head.size == 1)
        assert(splits.head.contains(beforeSplit1))
    }
    
    test("test toSmallSegments 2 element") {
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit1, beforeSplit2))
        
        assert(splits.size == 1)
        assert(splits.head.size == 2)
        assert(splits.head.contains(beforeSplit1))
        assert(splits.head.contains(beforeSplit2))
    }
    
    test("test toSmallSegments 2 small segments") {
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit1, beforeSplit2, beforeSplit3))
        
        assert(splits.size == 2)
        assert(splits.head.size == 2)
        assert(splits.head.contains(beforeSplit1))
        assert(splits.head.contains(beforeSplit2))
        assert(splits.last.size == 2)
        assert(splits.last.contains(beforeSplit3))
        assert(splits.last.contains(beforeSplit2))
    }
    
    test("test toSmallSegments 1 small segment") {
        val beforeSplit3 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:00:00"), 535.2D)
        val beforeSplit1 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:15:00"), 10535.2D)
        val beforeSplit2 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:30:00"), 20535.2D)
        
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit3, beforeSplit1, beforeSplit2))
        
        assert(splits.size == 1)
        assert(splits.head.size == 3)
        assert(splits.head.contains(beforeSplit1))
        assert(splits.head.contains(beforeSplit2))
        assert(splits.head.contains(beforeSplit3))
    }
    
    
    test("test toSmallSegments 1 small segment with margin") {
        val beforeSplit3 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:00:00"), 535.2D)
        val beforeSplit33 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:15:00"), 535.1D)
        val beforeSplit1 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:30:00"), 10535.2D)
        val beforeSplit2 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:45:00"), 20535.2D)
        
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit3, beforeSplit33, beforeSplit1, beforeSplit2))
        
        assert(splits.size == 1)
        assert(splits.head.size == 4)
        assert(splits.head.contains(beforeSplit1))
        assert(splits.head.contains(beforeSplit2))
        assert(splits.head.contains(beforeSplit3))
        assert(splits.head.contains(beforeSplit33))
    }
    
    test("test toSmallSegments 3 small segments") {
        val bs1 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:00:00"), 10535.2D)
        val bs2 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:15:00"), 10900.2D)
        val bs3 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:30:00"), 20535.2D) //cut position
        val bs4 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 00:45:00"), 10535.2D)
        val bs5 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 01:00:00"), 2000.2D)
        val bs6 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 01:15:00"), 535.2D) //cut position
        val bs7 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 01:30:00"), 2535.2D)
        val bs8 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 01:45:00"), 5535.2D)
        val bs9 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 02:00:00"), 10535.2D)
        val bs10 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 02:15:00"), 20535.2D)
        val bs11 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 02:30:00"), 30535.2D)
        val bs12 = BeforeSplit(TimestampHelper.getTimestamp("2019-07-04 02:45:00"), 40535.2D)
        val seq = Seq(bs1, bs2, bs3, bs4, bs5, bs6, bs7, bs8, bs9, bs10, bs11, bs12)
        
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(seq)
        
        splits.map(bss => bss.map(_.value)).foreach(println)
        assert(splits.size == 2)
        
        println("----")
        val splits2: Seq[Seq[BeforeSplit]] = splits.flatMap(Splitter.toSmallSegments)
        splits2.map(bss => bss.map(_.value)).foreach(println)
        assert(splits2.size == 3)
    
        println("----")
        val splits3: Seq[Seq[BeforeSplit]] = splits2.flatMap(Splitter.toSmallSegments)
        splits3.map(bss => bss.map(_.value)).foreach(println)
        assert(splits3.size == 3)
    }
}
