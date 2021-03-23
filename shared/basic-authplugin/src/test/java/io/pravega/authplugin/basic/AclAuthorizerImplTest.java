/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.authplugin.basic;

import io.pravega.auth.AuthHandler;
import io.pravega.common.Exceptions;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AclAuthorizerImplTest {

    private final static String DUMMY_ENCRYPTED_PWD = "Dummy encrypted value";

    @Test
    public void testSuperUserAcl() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::*", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();

        // Root resource
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScopes()));

        // Specific resources
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScope("testScope")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofStreamInScope("testScope", "testStream")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofReaderGroupInScope("testScope", "testRgt")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofKeyValueTableInScope("testScope", "testKvt")));
    }

    @Test
    public void testUserWithCreateScopeAccess() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScopes()));
    }

    @Test
    public void testUserWithAccessToASpecificScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/scope:testscope", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScope("testscope")));
    }

    @Test
    public void testUserWithAccessToAllScopes() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/scope:*", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScope("testscope1")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, ResourceRepresentation.ofScope("testscope2")));
    }

    @Test
    public void testUserWithAccessToAllDirectAndIndirectChildrenOfRoot() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/*", AuthHandler.Permissions.READ)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ, authorizer.authorize(acl, ResourceRepresentation.ofScope("testscope1")));
        assertEquals(AuthHandler.Permissions.READ, authorizer.authorize(acl, ResourceRepresentation.ofScope("testscope2")));
        assertEquals(AuthHandler.Permissions.READ, authorizer.authorize(acl, ResourceRepresentation.ofStreamInScope(
                "testscope", "teststream")));
    }

    @Test
    public void testUserWithAccessToAllDirectAndIndirectChildrenOfScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/scope:abcscope/*", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();

        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl, ResourceRepresentation.ofScope("abcscope")));
        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl, ResourceRepresentation.ofScope("xyzscope")));

        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "stream123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "stream456")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofReaderGroupInScope("abcscope", "rg123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofKeyValueTableInScope("abcscope", "kvt123")));

        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("xyzscope", "stream123")));
    }

    @Test
    public void testUserWithAccessToStreamPatternsInASpecificScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/scope:abcscope/stream:str*", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "str123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "streaMMMMMMMMMMM")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "str")));
    }

    @Test
    public void testUserWithAccessToStreamPatternsInAnyScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("prn::/scope:*/stream:mystream", AuthHandler.Permissions.READ_UPDATE)
        ));

        AclAuthorizerImpl authorizer = new AclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("abcscope", "mystream")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("mnoscope", "mystream")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                ResourceRepresentation.ofStreamInScope("xyzscope", "mystream")));
    }
}

class ResourceRepresentation {
    public static final String DOMAIN_PART_SUFFIX = "prn::";
    private static final String TAG_SCOPE = "scope";
    private static final String TAG_STREAM = "stream";
    private static final String TAG_READERGROUP = "reader-group";
    private static final String TAG_KEYVALUETABLE = "key-value-table";

    private static final String ROOT_RESOURCE = String.format("%s/", DOMAIN_PART_SUFFIX);

    public static String ofScopes() {
        return ROOT_RESOURCE;
    }

    public static String ofScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return String.format("%s/%s:%s", DOMAIN_PART_SUFFIX, TAG_SCOPE, scopeName);
    }

    public static String ofStreamsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    public static String ofStreamInScope(String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_STREAM, streamName);
    }

    public static String ofReaderGroupsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    public static String ofReaderGroupInScope(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_READERGROUP, readerGroupName);
    }

    public static String ofKeyValueTableInScope(String scopeName, String keyValueTableName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(keyValueTableName, "keyValueTableName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_KEYVALUETABLE, keyValueTableName);
    }
}
