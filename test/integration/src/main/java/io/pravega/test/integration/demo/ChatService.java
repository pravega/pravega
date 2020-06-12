/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.NameUtils;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * This demo implements a simple chat service using Pravega Streams, StreamCuts and Key-Value Tables.
 * <p/>
 * In this implementation:
 * - Streams are used to record messages sent in conversations.
 * - Stream Cuts are used to record how far has a user "read" a conversation.
 * - Key-Value Tables are used to hold metadata about conversations, users and what conversations a user is subscribed to
 * (as well as the position (Stream Cut) within those conversations.
 * - When a user "logs in", their conversation subscriptions are read from the Key-Value Table and the latest read message;
 * after that EventStreamReaders (Subscriptionlisteners) are used to tail the appropriate conversations after that point.
 * <p/>
 * Chat Service architecture:
 * - There is a set of Users with unique user names.
 * - There is a set of Public Channels with unique names.
 * - A Conversation is either a Public Channel or a direct-message conversation (between two or more users).
 * - Each Conversation (whether direct or Public Channel) is backed by a single Pravega Stream (1:1 mapping).
 * - A single Key-Value Table (String-String) is used for the following:
 * -- To store Users (Key Family "Users").
 * -- To store Public Channels (Key Family "Channels")
 * -- To store conversation subscriptions for each user. Each user gets a Key Family in the form "UserSubscription-{username}".
 * <p/>
 * How to use this:
 * - This is a command-line demo. The user types in instructions on the command line and they get executed.
 * - Syntax: {command-name} {arguments}
 * -- {command-name} is prefixed by "!" for actions and by {@code @} for messages that need to be sent.
 * <p>
 * The following are a list of commands
 * <p>
 * - !exit: Exits the demo application.
 * - !create-user {username}: Creates a new user with the given username.
 * - !create-channel {channelname}: Creates a new Public Channel with the given name.
 * - !list-users: List all the users in the system.
 * - !list-channels: Lists all channels in the system.
 * - !login: {username}: The user by the specified user name is now active in this session.
 * - !subscribe {channelname}: Subscribes the currently logged in user to the given channel. Until unsubscribed, all messages
 * published to this channel will be displayed as soon as they are published.
 * - !unsubscribe {channelname}: Unsubscribes the currently logged in user from the given channel.
 * - !list-subscriptions: Lists all subscriptions the currently logged in user has.
 * <p>
 * Example:
 * {@code
 * !create-user user1
 * !create-user user2
 * !create-channel ch1
 * !list-users
 * !list-channels
 * !login user1
 * !subscribe #ch1
 * @user2 Hey user2! How are you?
 * @#ch1 Hey everyone. This is the first message ever on this channel.
 * !login user2
 * @#ch1 Hey there. Everything is good.
 * !login user1
 * !exit
 * }
 */
public class ChatService {
    private static final String CHAT_SCOPE = "PravegaChatServiceDemo";
    private static final String METADATA_TABLE_NAME = "ChatMetadata";
    private static final String CHANNELS_KEY_FAMILY = "Conversations";
    private static final String USERS_KEY_FAMILY = "Users";
    private static final String USERS_SUBSCRIPTIONS_KEY_FAMILY_PREFIX = "UserSubscriptions-";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
    private static final AtomicReference<ChatClient> CHAT_CLIENT = new AtomicReference<>();

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega Chat Service Demo.");
        printHelp();
        try {
            executeCommand("connect", DEFAULT_CONTROLLER_URI);
            run();
        } finally {
            closeChatClient();
        }
        System.exit(0);
    }

    private static void printHelp() {
        System.out.println(String.format("Please refer to %s Javadoc for instructions and examples."));
    }

    private static void run() {
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            String line = input.nextLine();
            try {
                if (!processInput(line)) {
                    break;
                }
            } catch (IllegalArgumentException ex) {
                System.out.println("Invalid input: " + ex.getMessage());
                printHelp();
            } catch (Exception ex) {
                System.out.println("Error: " + ex.getMessage());
                printHelp();
            }
        }
    }

    private static void closeChatClient() {
        val c = CHAT_CLIENT.getAndSet(null);
        if (c != null) {
            c.close();
        }
    }

    private static ChatClient getChatClient() {
        val c = CHAT_CLIENT.get();
        Preconditions.checkState(c != null, "No connection established yet.");
        return c;
    }

    private static boolean processInput(String line) {
        line = line.trim();
        if (line.length() == 0) {
            return true;
        }

        int commandDelimPos = line.indexOf(" ");
        String command;
        String arg;
        if (commandDelimPos <= 0) {
            command = line;
            arg = null;
        } else {
            command = line.substring(0, commandDelimPos).trim().toLowerCase();
            arg = line.substring(commandDelimPos).trim();
        }

        if (command.startsWith("!")) {
            command = command.substring(1);
            return executeCommand(command, arg);
        } else if (command.startsWith("@")) {
            command = command.substring(1);
            getChatClient().getUserSession().sendMessage(command, arg);
            return true;
        }

        throw new IllegalArgumentException("Illegal start of command.");
    }

    private static boolean executeCommand(String command, String arg) {
        if (command.equals("exit")) {
            return false;
        }
        switch (command) {
            case "exit":
                return false;
            case "connect":
                val newClient = new ChatClient(URI.create(arg));
                try {
                    newClient.connect();
                    closeChatClient();
                    CHAT_CLIENT.set(newClient);
                } catch (Exception ex) {
                    newClient.close();
                    throw ex;
                }
                break;
            case "login":
                getChatClient().login(arg);
                break;
            case "create-user":
                getChatClient().createUser(arg);
                break;
            case "create-channel":
                getChatClient().createPublicChannel(arg);
                break;
            case "subscribe":
                getChatClient().getUserSession().subscribe(arg);
                break;
            case "unsubscribe":
                getChatClient().getUserSession().unsubscribe(arg);
                break;
            case "list-subscriptions":
                getChatClient().getUserSession().listSubscriptions();
                break;
            case "list-channels":
                getChatClient().listAllChannels();
                break;
            case "list-users":
                getChatClient().listAllUsers();
                break;
        }
        return true;
    }

    private static String getChannelName(String name) {
        return isPublicChannelName(name) ? name : "#" + name;
    }

    private static boolean isPublicChannelName(String name) {
        return name.startsWith("#");
    }

    private static String getChannelScopedStreamName(String channelName) {
        return NameUtils.getScopedStreamName(CHAT_SCOPE, getChannelStreamName(channelName));
    }

    private static String getChannelStreamName(String channelName) {
        return isPublicChannelName(channelName) ? channelName.substring(1) : channelName;
    }

    private static void reportError(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        if (ex instanceof InterruptedException) {
            return;
        }
        ex.printStackTrace();
    }

    //region ChatClient

    private static class ChatClient implements AutoCloseable {
        private final URI controllerUri;
        private final StreamManager streamManager;
        private final KeyValueTableManager keyValueTableManager;
        @Getter
        private final EventStreamClientFactory clientFactory;
        @Getter
        private final ReaderGroupManager readerGroupManager;
        private final KeyValueTableFactory keyValueTableFactory;
        private final AtomicReference<KeyValueTable<String, String>> metadataTable;
        private final AtomicReference<User> userSession;
        @Getter
        private final ScheduledExecutorService executor;

        ChatClient(@NonNull URI controllerUri) {
            this.controllerUri = controllerUri;
            this.streamManager = StreamManager.create(controllerUri);
            this.keyValueTableManager = KeyValueTableManager.create(controllerUri);
            val clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
            this.clientFactory = EventStreamClientFactory.withScope(CHAT_SCOPE, clientConfig);
            this.readerGroupManager = ReaderGroupManager.withScope(CHAT_SCOPE, clientConfig);
            this.keyValueTableFactory = KeyValueTableFactory.withScope(CHAT_SCOPE, clientConfig);
            this.metadataTable = new AtomicReference<>();
            this.userSession = new AtomicReference<>();
            this.executor = ExecutorServiceHelpers.newScheduledThreadPool(30, "chat-client");
        }

        @Override
        public void close() {
            closeUserSession();
            val mt = this.metadataTable.get();
            if (mt != null) {
                mt.close();
            }

            this.streamManager.close();
            this.readerGroupManager.close();
            this.keyValueTableManager.close();
            this.keyValueTableFactory.close();
            this.clientFactory.close();
            ExecutorServiceHelpers.shutdown(this.executor);
            System.out.println(String.format("Disconnected from '%s'.", this.controllerUri));
        }


        private void closeUserSession() {
            val session = this.userSession.getAndSet(null);
            if (session != null) {
                session.close();
            }
        }

        void connect() {
            System.out.println(String.format("Connecting to '%s' ...", this.controllerUri));
            val scopeCreated = this.streamManager.createScope(CHAT_SCOPE);
            if (scopeCreated) {
                System.out.println(String.format("\tScope '%s' created. There are no users or channels registered yet.", CHAT_SCOPE));
            } else {
                System.out.println(String.format("\tScope '%s' already exists. There may already be users or channels registered.", CHAT_SCOPE));
            }

            val kvtCreated = this.keyValueTableManager.createKeyValueTable(CHAT_SCOPE, METADATA_TABLE_NAME,
                    KeyValueTableConfiguration.builder().partitionCount(4).build());
            if (kvtCreated) {
                System.out.println(String.format("\tMetadata table '%s/%s' created.", CHAT_SCOPE, METADATA_TABLE_NAME));
            } else {
                System.out.println(String.format("\tMetadata table '%s/%s' already exists.", CHAT_SCOPE, METADATA_TABLE_NAME));
            }

            this.metadataTable.set(this.keyValueTableFactory.forKeyValueTable(METADATA_TABLE_NAME, new UTF8StringSerializer(),
                    new UTF8StringSerializer(), KeyValueTableClientConfiguration.builder().build()));

            System.out.println(String.format("Connected to '%s'.", this.controllerUri));
        }

        User getUserSession() {
            val u = this.userSession.get();
            Preconditions.checkArgument(u != null, "No user logged in yet.");
            return u;
        }

        KeyValueTableMap<String, String> getUserTable() {
            return this.metadataTable.get().getMapFor(USERS_KEY_FAMILY);
        }

        KeyValueTableMap<String, String> getChannelTable() {
            return this.metadataTable.get().getMapFor(CHANNELS_KEY_FAMILY);
        }

        KeyValueTableMap<String, String> getUserSubscriptionsTable(String userName) {
            return this.metadataTable.get().getMapFor(USERS_SUBSCRIPTIONS_KEY_FAMILY_PREFIX + userName);
        }

        void publish(String channelName, String fromUserName, String message) {
            val channelStreamName = getChannelStreamName(channelName);
            try (val w = this.clientFactory.createEventWriter(channelStreamName, new UTF8StringSerializer(), EventWriterConfig.builder().build())) {
                w.writeEvent(fromUserName, message);
            }
        }

        void login(String userName) {
            Exceptions.checkNotNullOrEmpty(userName, "userName");
            String userData = getUserTable().get(userName);
            if (userData == null) {
                System.out.println(String.format("No user with id '%s' is registered.", userName));
                return;
            }

            closeUserSession();
            this.userSession.set(new User(userName, this));
            System.out.println(String.format("User session for '%s' started.", userName));
            getUserSession().startListening();
        }

        void createUser(String userName) {
            Exceptions.checkNotNullOrEmpty(userName, "userName");
            val newData = Instant.now().toString();
            val oldData = getUserTable().putIfAbsent(userName, newData);
            if (oldData.equals(newData)) {
                System.out.println(String.format("User '%s' created successfully.", userName));
            } else {
                System.out.println(String.format("User '%s' already exists.", userName));
            }
        }

        void createPublicChannel(String channelName) {
            Exceptions.checkNotNullOrEmpty(channelName, "channelName");
            String channelStreamName = NameUtils.validateStreamName(channelName);
            channelName = getChannelName(channelStreamName); // Make sure it has the proper prefix.
            val newData = Instant.now().toString();
            val oldData = getChannelTable().putIfAbsent(channelName, newData);
            if (oldData.equals(newData)) {
                // Create a stream for this channel.
                boolean streamCreated = this.streamManager.createStream(CHAT_SCOPE, channelStreamName,
                        StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(4)).build());
                if (streamCreated) {
                    System.out.println(String.format("Channel '%s' created successfully. Stream name: '%s/%s'.", channelName,
                            CHAT_SCOPE, channelStreamName));
                } else {
                    getChannelTable().removeDirect(channelName);
                    System.out.println(String.format("Unable to create Stream '%s/%s' for channel '%s'.", CHAT_SCOPE,
                            channelStreamName, channelName));
                }
            } else {
                System.out.println(String.format("Channel '%s' already exists.", channelName));
            }
        }

        void createDirectMessageChannel(String channelName) {
            Exceptions.checkNotNullOrEmpty(channelName, "channelName");
            String channelStreamName = NameUtils.validateStreamName(channelName);
            val newData = Instant.now().toString();
            val oldData = getChannelTable().putIfAbsent(channelName, newData);
            if (oldData.equals(newData)) {
                // Create a stream for this channel.
                this.streamManager.createStream(CHAT_SCOPE, channelStreamName,
                        StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(4)).build());
                System.out.println(String.format("Created new direct channel '%s'.", channelName));
            }
        }

        void listAllChannels() {
            int count = 0;
            for (val channelName : getChannelTable().keySet()) {
                if (isPublicChannelName(channelName)) {
                    System.out.println(String.format("\t%s", channelName));
                    count++;
                }
            }

            System.out.println(String.format("Total channel count: %s.", count));

        }

        void listAllUsers() {
            int count = 0;
            for (val userName : getUserTable().keySet()) {
                System.out.println(String.format("\t%s", userName));
                count++;
            }

            System.out.println(String.format("Total user count: %s.", count));
        }
    }

    //endregion


    @Data
    private static class User implements AutoCloseable {
        private final String userName;
        private final ChatClient chatClient;
        private final AtomicReference<ScheduledFuture<?>> subscriptionListener = new AtomicReference<>();
        private final AtomicReference<Map<String, SubscriptionListener>> currentSubscriptions = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        void subscribe(String channelName) {
            channelName = getChannelName(channelName);
            Preconditions.checkArgument(this.chatClient.getChannelTable().containsKey(channelName), "Channel '%s' does not exist.", channelName);
            val st = this.chatClient.getUserSubscriptionsTable(this.userName);
            if (st.containsKey(channelName)) {
                System.out.println(String.format("User '%s' is already subscribed to '%s'.", this.userName, channelName));
            } else {
                st.putDirect(channelName, StreamCut.UNBOUNDED.asText());
                System.out.println(String.format("User '%s' has been subscribed to '%s'.", this.userName, channelName));
            }
        }

        void unsubscribe(String channelName) {
            Exceptions.checkNotNullOrEmpty(channelName, "channelName");
            val st = this.chatClient.getUserSubscriptionsTable(this.userName);
            val sc = st.remove(channelName);
            if (sc == null) {
                System.out.println(String.format("User '%s' is not subscribed to '%s'.", this.userName, channelName));
            } else {
                System.out.println(String.format("User '%s' has been unsubscribed from '%s'.", this.userName, channelName));
            }
        }

        void sendMessage(String target, String message) {
            String channelName = target;
            if (!target.startsWith("#")) {
                // Direct user message.
                channelName = Stream.of(target, this.userName).sorted().collect(Collectors.joining("-"));

                // Create a chat client, if needed.
                this.chatClient.createDirectMessageChannel(channelName);

                // Subscribe this user to this channel, if necessary
                this.chatClient.getUserSubscriptionsTable(this.userName).putIfAbsent(channelName, StreamCut.UNBOUNDED.asText());

                // Subscribe that user to this channel, if necessary.
                this.chatClient.getUserSubscriptionsTable(target).putIfAbsent(channelName, StreamCut.UNBOUNDED.asText());
            }

            // Publish the message.
            message = String.format("[%s] %s", this.userName, message);
            this.chatClient.publish(channelName, this.userName, message);
        }

        void listSubscriptions() {
            val subscriptions = this.currentSubscriptions.get();
            if (subscriptions == null || subscriptions.isEmpty()) {
                System.out.println("No subscriptions.");
                return;
            }

            System.out.println(String.format("Subscription count: %s.", subscriptions.size()));
            subscriptions.keySet().forEach(channelName -> System.out.println(String.format("\t%s", channelName)));
        }

        void startListening() {
            Preconditions.checkState(this.subscriptionListener.get() == null, "Already listening.");

            // Listen to all subscribed channels.
            // Periodically list all keys in KVT[UserSubscriptions-{Session.userName}] to detect new channels.
            this.subscriptionListener.set(this.chatClient.executor.scheduleWithFixedDelay(this::refreshSubscriptionList, 1000, 1000, TimeUnit.MILLISECONDS));
            System.out.println(String.format("Listening to all subscriptions for user '%s'.", this.userName));
        }

        private void refreshSubscriptionList() {
            val current = new HashSet<>(this.chatClient.getUserSubscriptionsTable(this.userName).keySet());
            val existing = this.currentSubscriptions.get() == null
                    ? new HashMap<String, SubscriptionListener>()
                    : new HashMap<>(this.currentSubscriptions.get());

            // Remove the ones we have since unsubscribed from.
            val removedSubscriptions = Sets.difference(existing.keySet(), current);
            removedSubscriptions.forEach(channelName -> existing.remove(channelName).close());

            // Add new subscriptions.
            val newSubscriptions = Sets.difference(current, existing.keySet());
            newSubscriptions.forEach(channelName -> {
                try {
                    existing.put(channelName, new SubscriptionListener(channelName, this.userName, this.chatClient));
                } catch (Exception ex) {
                    if (!this.closed.get()) {
                        System.out.println(String.format("Error: unable to start a listener for '%s': %s.", channelName, ex.toString()));
                        reportError(ex);
                    }
                }
            });
            this.currentSubscriptions.set(existing);
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                val sl = this.subscriptionListener.getAndSet(null);
                if (sl != null) {
                    sl.cancel(true);
                }

                val subscriptions = this.currentSubscriptions.getAndSet(null);
                if (subscriptions != null) {
                    subscriptions.values().forEach(SubscriptionListener::close);
                }

                System.out.println(String.format("User session for '%s' ended.", this.userName));
            }
        }
    }

    //region SubscriptionListener

    private static class SubscriptionListener implements AutoCloseable {
        private final String channelName;
        private final String userName;
        private final ChatClient client;
        private final EventStreamReader<String> reader;
        private final String readerGroupId;
        private final ScheduledFuture<?> readPoll;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        SubscriptionListener(@NonNull String channelName, @NonNull String userName, @NonNull ChatClient client) {
            this.channelName = channelName;
            this.userName = userName;
            this.client = client;

            val positionText = client.getUserSubscriptionsTable(userName).getOrDefault(channelName, null);
            val position = positionText == null ? StreamCut.UNBOUNDED : StreamCut.from(positionText);

            this.readerGroupId = UUID.randomUUID().toString().replace("-", "");
            val readerId = UUID.randomUUID().toString().replace("-", "");
            val rgConfig = ReaderGroupConfig.builder().stream(getChannelScopedStreamName(this.channelName), position).build();
            client.getReaderGroupManager().createReaderGroup(this.readerGroupId, rgConfig);
            this.reader = client.getClientFactory().createReader(readerId, this.readerGroupId, new UTF8StringSerializer(),
                    ReaderConfig.builder().build());

            // Stream cut points to the last event we read, so we don't want to display it again.
            if (readOnce() == 0) {
                System.out.println(String.format("You're all caught up on '%s'.", this.channelName));
            }

            this.readPoll = client.getExecutor().scheduleWithFixedDelay(this::readOnce, 100, 100, TimeUnit.MILLISECONDS);
        }

        private int readOnce() {
            try {
                //StreamCut lastStreamCut = null;
                int eventCount = 0;
                while (true) {
                    // Begin recording a Stream Cut. Whenever we are done we can record the position as having been read
                    // up to here.
                    beginStreamCut().whenComplete((streamCuts, ex) -> {
                        if (this.closed.get()) {
                            return;
                        }

                        if (ex != null) {
                            reportError(ex);
                        } else {
                            recordStreamCut(streamCuts);
                        }
                    });
                    EventRead<String> e = this.reader.readNextEvent(1000);
                    if (e.getEvent() == null) {
                        break;
                    }

                    System.out.println(String.format("> %s: %s", this.channelName, e.getEvent()));
                    eventCount++;
                }
                return eventCount;
            } catch (Exception ex) {
                if (!this.closed.get()) {
                    reportError(ex);
                }
                return -1;
            }
        }

        private CompletableFuture<Map<io.pravega.client.stream.Stream, StreamCut>> beginStreamCut() {
            return this.client.getReaderGroupManager().getReaderGroup(this.readerGroupId).generateStreamCuts(this.client.getExecutor());
        }

        private void recordStreamCut(Map<io.pravega.client.stream.Stream, StreamCut> streamCuts) {
            val lastStreamCut = streamCuts.values().stream().findFirst().orElse(null);
            this.client.getUserSubscriptionsTable(this.userName).put(this.channelName, lastStreamCut.asText());
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                this.readPoll.cancel(true);
                this.reader.close();
                this.client.getReaderGroupManager().deleteReaderGroup(this.readerGroupId);
                System.out.println(String.format("Stopped listening on '%s'.", this.channelName));
            }
        }
    }

    //endregion
}
