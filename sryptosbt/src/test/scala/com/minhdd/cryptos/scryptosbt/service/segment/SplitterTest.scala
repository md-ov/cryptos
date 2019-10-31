package com.minhdd.cryptos.scryptosbt.service.segment

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
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
        val splits: Seq[Seq[BeforeSplit]] = Splitter.toSmallSegments(Seq(beforeSplit3, beforeSplit1, beforeSplit2))
        
        assert(splits.size == 1)
        assert(splits.head.size == 3)
        assert(splits.head.contains(beforeSplit1))
        assert(splits.head.contains(beforeSplit2))
        assert(splits.head.contains(beforeSplit3))
    }

    
}
