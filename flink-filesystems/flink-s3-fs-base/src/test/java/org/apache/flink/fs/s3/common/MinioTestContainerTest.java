/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code MinioTestContainerTest} tests some basic functionality provided by {@link
 * MinioTestContainer}.
 */
class MinioTestContainerTest {

    private static final String DEFAULT_BUCKET_NAME = "test-bucket";

    @RegisterExtension
    private static final EachCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new EachCallbackWrapper<>(
                            new TestContainerExtension<>(
                                    () -> new MinioTestContainer(DEFAULT_BUCKET_NAME)));

    private static MinioTestContainer getTestContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }

    private static S3AsyncClient getClient() {
        return getTestContainer().getClient();
    }

    private static Bucket createBucket(String bucketName) {
        try {
            return getClient()
                    .createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
                    .whenComplete(
                            (response, throwable) -> {
                                if (throwable != null) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Bucket '%s' creation failed", bucketName),
                                            throwable);
                                }
                            })
                    .thenCompose(
                            createBucketResponse ->
                                    getClient()
                                            .waiter()
                                            .waitUntilBucketExists(
                                                    HeadBucketRequest.builder()
                                                            .bucket(bucketName)
                                                            .build()))
                    .thenCompose(
                            waitForBucketResponse -> {
                                return CompletableFuture.supplyAsync(
                                        () -> {
                                            for (Bucket bucket : listBuckets()) {
                                                if (bucket.name().equals(bucketName)) {
                                                    return bucket;
                                                }
                                            }
                                            throw new RuntimeException(
                                                    String.format(
                                                            "Bucket '%s' not found", bucketName));
                                        });
                            })
                    .get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static List<Bucket> listBuckets() {
        try {
            return getClient().listBuckets().get().buckets();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testBucketCreation() throws ExecutionException, InterruptedException {
        final String bucketName = "other-bucket";
        final Bucket otherBucket = createBucket(bucketName);

        assertThat(otherBucket).isNotNull();
        assertThat(otherBucket).extracting(Bucket::name).isEqualTo(bucketName);

        assertThat(listBuckets())
                .map(Bucket::name)
                .containsExactlyInAnyOrder(getTestContainer().getDefaultBucketName(), bucketName);
    }

    @Test
    void testPutObject() throws IOException, ExecutionException, InterruptedException {
        final String bucketName = "other-bucket";

        createBucket(bucketName);
        final String objectId = "test-object";
        final String content = "test content";
        getClient()
                .putObject(
                        PutObjectRequest.builder().bucket(bucketName).key(objectId).build(),
                        AsyncRequestBody.fromString(content));

        final BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                getClient()
                                        .getObject(
                                                GetObjectRequest.builder()
                                                        .bucket(bucketName)
                                                        .key(objectId)
                                                        .build(),
                                                AsyncResponseTransformer.toBlockingInputStream())
                                        .get()));
        assertThat(reader.readLine()).isEqualTo(content);
    }

    @Test
    void testSetS3ConfigOptions() {
        final Configuration config = new Configuration();
        getTestContainer().setS3ConfigOptions(config);

        assertThat(config.containsKey("s3.endpoint")).isTrue();
        assertThat(config.containsKey("s3.path.style.access")).isTrue();
        assertThat(config.containsKey("s3.access-key")).isTrue();
        assertThat(config.containsKey("s3.secret-key")).isTrue();
    }

    @Test
    void testGetDefaultBucketName() {
        assertThat(getTestContainer().getDefaultBucketName()).isEqualTo(DEFAULT_BUCKET_NAME);
    }

    @Test
    void testDefaultBucketCreation() {
        assertThat(listBuckets())
                .singleElement()
                .extracting(Bucket::name)
                .isEqualTo(getTestContainer().getDefaultBucketName());
    }

    @Test
    void testS3EndpointNeedsToBeSpecifiedBeforeInitializingFileSyste() {
        assertThatThrownBy(() -> getTestContainer().initializeFileSystem(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
