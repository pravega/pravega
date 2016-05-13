package com.emc.logservice.TestImplementations;

import com.emc.logservice.Core.ByteArraySegment;
import com.emc.logservice.Logs.DataFrame;
import com.emc.logservice.Logs.DataFrameLog;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/**
 * Created by padura on 4/15/16.
 */
public class InMemoryDataFrameLog implements DataFrameLog
{
    private final LinkedList<Entry> entries;
    private long offset;
    private int lastMagic;
    private final long delayMillisPerMB;

    public InMemoryDataFrameLog()
    {
        this(0);
    }

    public InMemoryDataFrameLog(long delayMillisPerMB)
    {
        this.entries = new LinkedList<>();
        this.offset = 0;
        this.lastMagic = -1;
        this.delayMillisPerMB = delayMillisPerMB;
    }

    @Override
    public CompletableFuture<Long> add(DataFrame dataFrame, Duration timeout)
    {
        long offset;
        ByteArraySegment bytes = dataFrame.getData();
        synchronized (this.entries)
        {
            if (this.entries.size() > 0 && dataFrame.getStartMagic() != this.lastMagic)
            {
                throw new IllegalArgumentException("Given DataFrame StartMagic does not match the last received frame's EndMagic.");
            }

            offset = this.offset;
            this.offset += bytes.getLength();
            this.entries.add(new Entry(offset, bytes));
            this.lastMagic = dataFrame.getEndMagic();
        }

        dataFrame.setFrameSequence(offset);

        long delayMillis = this.delayMillisPerMB * bytes.getLength() / 1024 / 1024;
        if (delayMillis > 0)
        {
            try
            {
                Thread.sleep(delayMillis);
            }
            catch (InterruptedException ex)
            {
            }
        }

        return CompletableFuture.completedFuture(offset);
    }

    @Override
    public CompletableFuture<Void> truncate(Long offset, Duration timeout)
    {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        // Run in a new thread to "simulate" asynchronous behavior.
        Thread t = new Thread(() ->
        {
            synchronized (this.entries)
            {
                while (this.entries.size() > 0 && this.entries.getFirst().offset + this.entries.getFirst().data.length <= offset)
                {
                    this.entries.removeFirst();
                }
            }

            resultFuture.complete(null);
        });

        t.start();
        return resultFuture;
    }

    @Override
    public CompletableFuture<Iterator<DataFrame>> read(Long afterOffset, int maxCount, Duration timeout)
    {
        CompletableFuture<Iterator<DataFrame>> resultFuture = new CompletableFuture<>();

        // Run in a new thread to "simulate" asynchronous behavior.
        Thread t = new Thread(() -> {
            // TODO: should we block if we have no data?
            LinkedList<DataFrame> result = new LinkedList<>();
            synchronized (this.entries)
            {
                for (Entry e : this.entries)
                {
                    if (e.offset > afterOffset)
                    {
                        try
                        {
                            DataFrame d = new DataFrame(e.data);
                            d.setFrameSequence(e.offset);
                            result.add(d);
                        }
                        catch (Exception ex)
                        {
                            resultFuture.completeExceptionally(ex);
                            return;
                        }

                        if (result.size() >= maxCount)
                        {
                            break;
                        }
                    }
                }
            }

            resultFuture.complete(result.iterator());
        });

        t.start();
        return resultFuture;
    }

    @Override
    public CompletableFuture<Void> recover(Duration timeout)
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getLastMagic()
    {
        return this.lastMagic;
    }

    private class Entry
    {
        public final long offset;
        public final byte[] data;

        public Entry(long offset, ByteArraySegment data)
        {
            this.offset = offset;
            this.data = new byte[data.getLength()];
            data.copyTo(this.data, 0, this.data.length);
        }
    }
}
