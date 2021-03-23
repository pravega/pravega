<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Working with Pravega: Transactions

This article explores how to write a set of Events to a Stream atomically using
Pravega Transactions.

Instructions for running the sample applications can be found in the[ Pravega
Samples
readme](https://github.com/pravega/pravega-samples/blob/v0.5.0/pravega-client-examples/README.md).

You really should be familiar with Pravega Concepts (see [Pravega
Concepts](pravega-concepts.md)) before continuing reading this page.

## Pravega Transactions and the Console Writer and Console Reader Apps

We have written a couple of applications, ConsoleReader and ConsoleWriter that
help illustrate reading and writing data with Pravega and in particular to
illustrate the Transaction facility in the Pravega programming model.  You can
find those applications
[here](https://github.com/pravega/pravega-samples/tree/v0.5.0/pravega-client-examples/src/main/java/io/pravega/example/consolerw).

### ConsoleReader

The ConsoleReader app is very simple.  It uses the Pravega Java Client Library
to read from a Stream and output each Event onto the console.  It runs
indefinitely, so you have to kill the process to terminate the program.

### ConsoleWriter

The ConsoleWriter app is a bit more sophisticated.  It uses the Pravega Java
Client Library to write Events to a Stream, including Events written in the
context of a Pravega Transaction.  To make manipulating Transactions a bit
easier, we provide a console-based CLI.  The help text for the CLI is shown
below:

**ConsoleWriter Help text**
```
Enter one of the following commands at the command line prompt:

If no command is entered, the line is treated as a parameter to the WRITE_EVENT command.

WRITE_EVENT {event} - write the {event} out to the Stream or the current Transaction.
WRITE_EVENT_RK <<{routingKey}>> , {event} - write the {event} out to the Stream or the current Transaction using {routingKey}. Note << and >> around {routingKey}.
BEGIN - begin a Transaction. Only one Transaction at a time is supported by the CLI.
GET_TXN_ID - output the current Transaction's Id (if a Transaction is running)
FLUSH - flush the current Transaction (if a Transaction is running)
COMMIT - commit the Transaction (if a Transaction is running)
ABORT - abort the Transaction (if a Transaction is running)
STATUS - check the status of the Transaction(if a Transaction is running)
HELP - print out a list of commands.
QUIT - terminate the program.

examples/someStream >
```

So writing a single Event is simple, just type some text (you don't even have to
type the WRITE\_EVENT command if you don't want to).

But we really want to talk about Pravega Transactions, so lets dive into that.

Pravega Transactions
====================

The idea with a Pravega Transaction is that it allows an application to prepare
a set of Events that can be written "all at once" to a Stream.  This allows an
application to "commit" a bunch of Events Atomically. This is done by writing them into the Transaction
and calling commit to append them to the Stream.  An application might
want to do this in cases where it wants the Events to be durably stored and
later decided whether or not those Events should be
appended to the Stream.  This allows the application
to control when the set of Events are made visible to Readers.

A Transaction is created via an EventStreamWriter.  Recall that an
EventStreamWriter itself is created through a ClientFactory and is constructed
to operate against a Stream.  Transactions are therefore bound to a Stream.
 Once a Transaction is created, it acts a lot like a Writer.  Applications Write
Events to the Transaction and once acknowledged, the data is considered durably
persisted in the Transaction.  Note that the data written to a Transaction will
not be visible to Readers until the Transaction is committed.  In addition to
writeEvent and writeEvent using a routing key, there are several Transaction
specific operations provided:

| **Operation** | **Discussion**                                                                                                                                          |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| getTxnId()    | Retrieve the unique identifier for the Transaction.                                                                                                     |
|               | Pravega generates a unique UUID for each Transaction.                                                                                                                                                        |
| flush()       | Ensure that all Writes have been persisted.                                                                                                             |
| ping()        | Extend the duration of a Transaction.                                                                                                                   |
|               | Note that after a certain amount of idle time, the Transaction will automatically abort. This is to handle the case where the client has crashed and it is no longer appropriate to keep resources associated with the Transaction.                                                                                                                                                        |
| checkStatus() | Return the state of the Transaction. The Transaction can be in one of the following states: Open, Committing, Committed, Aborting or Aborted.           |
| commit()      | Append all of the Events written to the Transaction into the Stream. Either all of the Event data will be appended to the Stream or none of it will be. |
| abort()       | Terminate the Transaction, the data written to the Transaction will be deleted.                                                                         |

## Using the ConsoleWriter to Begin and Commit a Transaction

All of the Transaction API is reflected in the ConsoleWriter's CLI command set.

To begin a transaction, type BEGIN:

**Begin Transaction**
```
examples/someStream >begin
346d8561-3fd8-40b6-8c15-9343eeea2992 >
```

When a Transaction is created, it returns a Transaction object parameterized to
the type of Event supported by the Stream.  In the case of the ConsoleWriter,
the type of Event is a Java String.

The command prompt changes to show the Transaction's id.  Now any of the
Transaction related commands can be issued (GET\_TXN\_ID, FLUSH, PING, COMMIT,
ABORT and STATUS).  Note that the BEGIN command won't work because the
ConsoleWriter supports only one Transaction at a time (this is a limitation of
the app, not a limitation of Pravega).  When the ConsoleWriter is in a
Transactional context, the WRITE\_EVENT (remember if you don't type a command,
ConsoleWriter assumes you want to write the text as an Event) or the
WRITE\_EVENT\_RK will be written to the Transaction:

**Write Events to a Transaction**
```
346d8561-3fd8-40b6-8c15-9343eeea2992 >m1
**** Wrote 'm1'
346d8561-3fd8-40b6-8c15-9343eeea2992 >m2
**** Wrote 'm2'
346d8561-3fd8-40b6-8c15-9343eeea2992 >m3
**** Wrote 'm3'
```

At this point, if you look at the Stream (by invoking the ConsoleReader app on
the Stream, for example), you won't see those Events written to the Stream.

**Events not Written to the Stream (yet)**
```
$ bin/consoleReader
...
******** Reading events from examples/someStream
```

But when a COMMIT command is given, causing the Transaction to commit:

**Do the Commit**
```
346d8561-3fd8-40b6-8c15-9343eeea2992 >commit
**** Transaction commit completed.
```
 those Events are appended to the Stream and are now all available:

**After commit, the Events are Visible**

```
******** Reading events from examples/someStream
'm1'
'm2'
'm3'
```

### More on Begin Transaction

The Begin Transaction (beginTxn()) operation takes three parameters
(ConsoleWriter chooses some reasonable defaults so in the CLI these are
optional): 

| **Param**          | **Discussion**                                                                                                        |
|--------------------|-----------------------------------------------------------------------------------------------------------------------|
| transactionTimeout | The amount of time a transaction should be allowed to run before it is automatically aborted by Pravega.              |
|                    | This is also referred to as a "lease".                                                                                                                      |
| maxExecutionTime   | The amount of time allowed between ping operations.                                                                   |
