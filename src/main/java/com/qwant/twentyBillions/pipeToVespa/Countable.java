package com.qwant.twentyBillions.pipeToVespa;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.vespa.http.client.FeedClient;

abstract class Countable {
	public static AtomicInteger nbMessagesSinceLastSecond = new AtomicInteger(0);
	public static AtomicInteger nbMessagesTotal = new AtomicInteger(0);
}
