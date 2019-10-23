package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.domain.Crypto
import org.apache.spark.sql.Dataset

object DatasetHelper {
    
    def union(ds1: Option[Dataset[Crypto]], ds2: Option[Dataset[Crypto]]): Option[Dataset[Crypto]] = {
        if (ds1.isEmpty && ds2.isEmpty) None
        else if (ds1.isEmpty) ds2
        else if (ds2.isEmpty) ds1
        else Some(ds1.get.union(ds2.get))
    }
    
}
