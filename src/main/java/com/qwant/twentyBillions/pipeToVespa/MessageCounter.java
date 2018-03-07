package com.qwant.twentyBillions.pipeToVespa;

import java.util.TimerTask;

public class MessageCounter<T extends Countable> extends TimerTask {
	public void run() {
		T.nbMessagesTotal.addAndGet(T.nbMessagesSinceLastSecond.get());
		System.out.println(T.nbMessagesSinceLastSecond + " messages per second, total: "
				+ T.nbMessagesTotal);
		T.nbMessagesSinceLastSecond.set(0);
	}
}
