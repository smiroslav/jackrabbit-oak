/*
 * ADOBE CONFIDENTIAL
 *
 * Copyright (c) 2024 Adobe
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe and its suppliers, if any. The intellectual
 * and technical concepts contained herein are proprietary to Adobe
 * and its suppliers and are protected by all applicable intellectual
 * property laws, including trade secret and copyright laws.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe.
 */
package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

public class ParseJsopUserManagerDiff {

    private ParseJsopUserManagerDiff() {}

    public static void applyJsopDiff(@NotNull String jsop, Session session, BlobStore blobStore) throws RepositoryException {
        if (jsop.trim().isEmpty()) {
            return;
        }
        UserManager userManager = ((JackrabbitSession)session).getUserManager();
        ValueFactory valueFactory = session.getValueFactory();
        CustomJsonDeserializer deserializer = new CustomJsonDeserializer(userManager, valueFactory);
        deserializer.applyJsop(jsop);
    }
}
