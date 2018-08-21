package com.minhdd.cryptos.scryptosbt.tools

object Seqs {
    def separate[A](seq: Seq[A], predicate: A => Boolean): (Seq[A], Seq[A]) = {
        val seq1 = seq.filter(predicate)
        val seq2 = seq.filter(!predicate(_))
        (seq1, seq2)
    }
}
