package org.apache.jackrabbit.oak.security.authentication.sync;

class QueueEntry {
    private final String payload;
    private final String initiatorId;

    QueueEntry(String payload, String initiatorId) {
        this.payload = payload;
        this.initiatorId = initiatorId;
    }

    public String getPayload() {
        return payload;
    }

    public String getInitiatorId() {
        return initiatorId;
    }
}
