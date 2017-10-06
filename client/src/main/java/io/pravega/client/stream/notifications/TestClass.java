package io.pravega.client.stream.notifications;

public class TestClass {

    public static void main(String[] args) {
        NotificationSystem system = NotificationSystem.INSTANCE;
        ScaleEventNotification notification = new ScaleEventNotification();

        notification.addListener(event -> {
            int numReader = event.getNumOfReaders();
            int segments = event.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
        });

        CustomEventNotification customNotify = new CustomEventNotification();
        customNotify.addListener(event -> {
            System.out.println("Custom Event notified");
        });

        //
        system.notify(new ScaleEvent(4, 5));
        system.notify(new CustomEvent());
    }
}
