package com.elastacloud.hive

import com.macasaet.fernet.{Key, Token}
import org.apache.hadoop.hive.ql.exec.UDF
import scala.collection.JavaConverters._
import scala.util._

@deprecated
class Decrypt extends UDF{
    def evaluate(inputVal: String, sparkKeys : String): String = {
        if( inputVal != null || inputVal!="" ) {
            Try {
              val keys = sparkKeys.split(",").map(sk => new Key(sk)).toList.asJava
              val token = Token.fromString(inputVal)
              token.validateAndDecrypt(keys, new Validator)
            } match {
                case Success(v) => v
                case Failure(f) => inputVal
            } 
        }else inputVal
    }
}
