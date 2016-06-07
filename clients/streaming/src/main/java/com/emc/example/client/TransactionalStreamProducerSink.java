package com.emc.example.client;

import java.util.Map;

import com.emc.example.client.dummy.RichSinkFunction;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class TransactionalStreamProducerSink<IN> extends RichSinkFunction<IN> // ...
{
	//
	private Transaction<IN> openTxn;
	Producer<IN> producer;

	public void invoke(IN value) throws TxFailedException {
		if (openTxn == null) {
			openTxn = producer.startTransaction( 60000);
			
		}
		String routingKey = getRoutingKey(value);
		openTxn.publish(routingKey, value);
	}

	// ...
	public void notifyCheckpointComplete() throws TxFailedException {
			openTxn.commit();
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