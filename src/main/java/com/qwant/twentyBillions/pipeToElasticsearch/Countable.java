package com.qwant.twentyBillions.pipeToElasticsearch;

import java.util.concurrent.atomic.AtomicInteger;

abstract class Countable {
	public static AtomicInteger nbMessagesSinceLastSecond = new AtomicInteger(0);
	public static AtomicInteger nbMessagesTotal = new AtomicInteger(0);
	public static AtomicInteger bulksInParallel = new AtomicInteger(0);

}
