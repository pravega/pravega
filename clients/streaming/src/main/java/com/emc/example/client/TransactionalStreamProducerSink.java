package com.emc.example.client;

import java.util.Map;

import com.emc.example.client.dummy.RichSinkFunction;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class TransactionalStreamProducerSink<IN> extends RichSinkFunction<IN> // ...
{
	// …
	private Map<String, Transaction<IN>> openTxns;
	Producer<IN> producer;

	public void invoke(IN value) throws TxFailedException {
		String routingKey = getRoutingKey(value);
		Transaction<IN> tx = openTxns.get(routingKey);
		if (tx == null) {
			tx = producer.startTransaction(routingKey, 60000);
			openTxns.put(routingKey, tx);
		}
		tx.publish(value);
	}

	// ...
	public void notifyCheckpointComplete() throws TxFailedException {
		for (Transaction<IN> t : openTxns.values()) {
			t.commit();
		}
	}

	public byte[] snapshotState() {
		return serializeOpenTxns();
	}

	public void restoreState(byte[] state) {
		restoreOpenTxns(state);
	}

	// ...
	private String getRoutingKey(IN value) {
		// TODO Auto-generated method stub
		return null;
	}

	private byte[] serializeOpenTxns() {
		// TODO Auto-generated method stub
		return null;
	}

	private void restoreOpenTxns(byte[] state) {
		// TODO Auto-generated method stub

	}
}