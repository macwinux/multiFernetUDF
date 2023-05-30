package com.example

import com.macasaet.fernet.StringValidator

import java.time.{Duration, Instant}

package object hive {
  class Validator extends StringValidator {
    override def getTimeToLive(): java.time.temporal.TemporalAmount = {
      Duration.ofSeconds(Instant.MAX.getEpochSecond());
    }
  }

}
