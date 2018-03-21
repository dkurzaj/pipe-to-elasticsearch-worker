package com.qwant.twentyBillions.pipeToElasticsearch;

import java.util.TimerTask;

public class MessageCounter<T extends Countable> extends TimerTask {
	@Override
	public void run() {
		T.nbMessagesTotal.addAndGet(T.nbMessagesSinceLastSecond.get());
		System.out.println(T.nbMessagesSinceLastSecond + " messages per second, total: "
				+ T.nbMessagesTotal + ", bulks in parrallel: " + T.bulksInParallel);
		T.nbMessagesSinceLastSecond.set(0);
	}
}
