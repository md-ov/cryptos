package com.minhdd.cryptos.scryptosbt.tools

object SeqHelper {

  def splitWithOffset[T](seq: Seq[T], offset: Int, numberOfSplit: Int): Seq[(Seq[T], Int)] = {
    if (numberOfSplit == 1 || numberOfSplit >= seq.length/2) {
      Seq((seq, offset))
    } else {
      val parts = seq.splitAt(seq.length / numberOfSplit)
      Seq((parts._1, offset)) ++ splitWithOffset(parts._2, offset + parts._1.length, numberOfSplit - 1)
    }
  }
}
