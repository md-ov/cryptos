package com.minhdd.cryptos.scryptosbt.tools

import scala.util.{Failure, Try}



object Numbers {
    implicit class TryOps[T](val t: Try[T]) extends AnyVal {
        def mapException(f: Throwable => Throwable): Try[T] = {
            t.recoverWith({ case e => Failure(f(e)) })
        }
    }
    
    def toDouble(s: String): Double = {
        s.toDouble
    }
    
    def twoDigit(i: String): String = {
        if (i.length == 1) "0" + i
        else i.toString
    }
    
    def fromStringToInt(s: String): Option[Int] = {
        Try {
            s.toInt
        }.mapException(e => new Exception("not a number", e)).toOption
    }
}
