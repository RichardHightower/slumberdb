package org.boon.slumberdb.entries;


public class UpdateStatus {

    final boolean successful;
    final VersionKey versionKey;

    public UpdateStatus(boolean successful, VersionKey versionKey) {
        this.successful = successful;
        this.versionKey = versionKey;
    }

    public UpdateStatus(VersionKey versionKey) {
        this.successful = false;
        this.versionKey = versionKey;
    }
}
