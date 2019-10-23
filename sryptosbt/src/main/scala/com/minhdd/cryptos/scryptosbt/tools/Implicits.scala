package com.minhdd.cryptos.scryptosbt.tools

import scala.util.{Failure, Try}

object Implicits {
    
    implicit class TryImplicit[T](val t: Try[T]) extends AnyVal {
        def mapException(f: Throwable => Throwable): Try[T] = {
            t.recoverWith({ case e => Failure(f(e)) })
        }
    }
    
}
