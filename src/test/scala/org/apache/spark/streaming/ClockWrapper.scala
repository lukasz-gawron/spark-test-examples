package org.apache.spark.streaming

/**
  * Created by lukasz.gawron on 14/06/2018.
  */
// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import org.apache.spark.util.ManualClock


/** Ugly hack to access Spark private ManualClock class. */
object ClockWrapper {
  def advance(ssc: StreamingContext, timeToAdd: Duration): Unit = {
    val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    manualClock.waitTillTime(timeToAdd.milliseconds)
  }
}
