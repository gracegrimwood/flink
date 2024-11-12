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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3control.model.S3ObjectMetadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link RecoverableMultiPartUploadImpl}. */
class RecoverableMultiPartUploadImplTest {

    private static final int BUFFER_SIZE = 10;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    @TempDir File temporaryFolder;

    private StubMultiPartUploader stubMultiPartUploader;

    private RecoverableMultiPartUploadImpl multiPartUploadUnderTest;

    @BeforeEach
    void before() throws IOException {
        stubMultiPartUploader = new StubMultiPartUploader();
        multiPartUploadUnderTest =
                RecoverableMultiPartUploadImpl.newUpload(
                        stubMultiPartUploader, new MainThreadExecutor(), TEST_OBJECT_NAME);
    }

    @Test
    void singlePartUploadShouldBeIncluded() throws IOException {
        final byte[] part = bytesOf("hello world");

        uploadPart(part);

        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, part, 1);
    }

    @Test
    void incompletePartShouldBeUploadedAsIndividualObject() throws IOException {
        final byte[] incompletePart = bytesOf("Hi!");

        uploadObject(incompletePart);

        assertThatHasUploadedObject(stubMultiPartUploader, incompletePart);
    }

    @Test
    void multiplePartAndObjectUploadsShouldBeIncluded() throws IOException {
        final byte[] firstCompletePart = bytesOf("hello world");
        final byte[] secondCompletePart = bytesOf("hello again");
        final byte[] thirdIncompletePart = bytesOf("!!!");

        uploadPart(firstCompletePart);
        uploadPart(secondCompletePart);
        uploadObject(thirdIncompletePart);

        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, firstCompletePart, 1);
        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, secondCompletePart, 2);
        assertThatHasUploadedObject(stubMultiPartUploader, thirdIncompletePart);
    }

    @Test
    void multiplePartAndObjectUploadsShouldBeReflectedInRecoverable() throws IOException {
        final byte[] firstCompletePart = bytesOf("hello world");
        final byte[] secondCompletePart = bytesOf("hello again");
        final byte[] thirdIncompletePart = bytesOf("!!!");

        uploadPart(firstCompletePart);
        uploadPart(secondCompletePart);

        final S3Recoverable recoverable = uploadObject(thirdIncompletePart);

        assertThatIsEqualTo(
                recoverable, thirdIncompletePart, firstCompletePart, secondCompletePart);
    }

    @Test
    void s3RecoverableReflectsTheLatestPartialObject() throws IOException {
        final byte[] incompletePartOne = bytesOf("AB");
        final byte[] incompletePartTwo = bytesOf("ABC");

        S3Recoverable recoverableOne = uploadObject(incompletePartOne);
        S3Recoverable recoverableTwo = uploadObject(incompletePartTwo);

        assertThat(recoverableTwo.incompleteObjectName())
                .isNotEqualTo(recoverableOne.incompleteObjectName());
    }

    @Test
    void uploadingNonClosedFileAsCompleteShouldThroughException() throws IOException {
        final byte[] incompletePart = bytesOf("!!!");

        final RefCountedBufferingFileStream incompletePartFile = writeContent(incompletePart);

        assertThatThrownBy(() -> multiPartUploadUnderTest.uploadPart(incompletePartFile))
                .isInstanceOf(IllegalStateException.class);
    }

    private static void assertThatHasMultiPartUploadWithPart(
            StubMultiPartUploader actual, byte[] content, int partNo) {
        UploadPartResponse expectedCompletePart =
                createUploadPartResult(TEST_OBJECT_NAME, partNo, content);

        assertThat(actual.getCompletePartsUploaded()).contains(expectedCompletePart);
    }

    private static void assertThatHasUploadedObject(StubMultiPartUploader actual, byte[] content) {
        PutObjectResponse expectedIncompletePart = createPutObjectResult(TEST_OBJECT_NAME, content);

        assertThat(actual.getIncompletePartsUploaded()).contains(expectedIncompletePart);
    }

    private static void assertThatIsEqualTo(
            S3Recoverable actualRecoverable, byte[] incompletePart, byte[]... completeParts) {
        S3Recoverable expectedRecoverable = createS3Recoverable(incompletePart, completeParts);

        assertThat(actualRecoverable.getObjectName())
                .isEqualTo(expectedRecoverable.getObjectName());
        assertThat(actualRecoverable.uploadId()).isEqualTo(expectedRecoverable.uploadId());
        assertThat(actualRecoverable.numBytesInParts())
                .isEqualTo(expectedRecoverable.numBytesInParts());
        assertThat(actualRecoverable.incompleteObjectLength())
                .isEqualTo(expectedRecoverable.incompleteObjectLength());

        assertThat(actualRecoverable.parts().stream().map(Part::eTag).toArray())
                .isEqualTo(expectedRecoverable.parts().stream().map(Part::eTag).toArray());
    }

    // ---------------------------------- Test Methods -------------------------------------------

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static S3Recoverable createS3Recoverable(
            byte[] incompletePart, byte[]... completeParts) {
        final List<Part> eTags = new ArrayList<>();

        int index = 1;
        long bytesInPart = 0L;
        for (byte[] part : completeParts) {
            eTags.add(
                    Part.builder()
                            .partNumber(index)
                            .eTag(createETag(TEST_OBJECT_NAME, index))
                            .build());
            bytesInPart += part.length;
            index++;
        }

        return new S3Recoverable(
                TEST_OBJECT_NAME,
                createMPUploadId(TEST_OBJECT_NAME),
                eTags,
                bytesInPart,
                "IGNORED-DUE-TO-RANDOMNESS",
                (long) incompletePart.length);
    }

    private static PutObjectResponse createPutObjectResult(String key, byte[] content) {
        final PutObjectResponse result = PutObjectResponse.builder().build();
        Class<?> innerClass = PutObjectResponse.class.getDeclaredClasses()[0];
        //        result.setETag(createETag(key, -1));
        //        result.setContent(content);
        return result;
    }

    private static UploadPartResponse createUploadPartResult(
            String key, int number, byte[] payload) {
        final UploadPartResponse result = UploadPartResponse.builder().build();
        //        result.setETag(createETag(key, number));
        //        result.setPartNumber(number);
        //        result.setContent(payload);
        return result;
    }

    private static String createMPUploadId(String key) {
        return "MPU-" + key;
    }

    private static String createETag(String key, int partNo) {
        return "ETAG-" + key + '-' + partNo;
    }

    private S3Recoverable uploadObject(byte[] content) throws IOException {
        final RefCountedBufferingFileStream incompletePartFile = writeContent(content);
        incompletePartFile.flush();

        // as in the production code, we assume that a file containing
        // a in-progress part is flushed but not closed before being passed
        // to the uploader.

        return multiPartUploadUnderTest.snapshotAndGetRecoverable(incompletePartFile);
    }

    private void uploadPart(final byte[] content) throws IOException {
        RefCountedBufferingFileStream partFile = writeContent(content);

        // as in the production code, we assume that a file containing
        // a completed part is closed before being passed to the uploader.

        partFile.close();

        multiPartUploadUnderTest.uploadPart(partFile);
    }

    private RefCountedBufferingFileStream writeContent(byte[] content) throws IOException {
        final File newFile = new File(temporaryFolder, ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedBufferingFileStream testStream =
                new RefCountedBufferingFileStream(
                        RefCountedFileWithStream.newFile(newFile, out), BUFFER_SIZE);

        testStream.write(content, 0, content.length);
        return testStream;
    }

    // ---------------------------------- Test Classes -------------------------------------------

    /** A simple executor that executes the runnable on the main thread. */
    private static class MainThreadExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    /**
     * A {@link S3AccessHelper} that simulates uploading part files to S3 by simply putting complete
     * and incomplete part files in lists for further validation.
     */
    private static class StubMultiPartUploader implements S3AccessHelper {

        private final List<UploadPartResponse> completePartsUploaded = new ArrayList<>();
        private final List<PutObjectResponse> incompletePartsUploaded = new ArrayList<>();

        List<UploadPartResponse> getCompletePartsUploaded() {
            return completePartsUploaded;
        }

        List<PutObjectResponse> getIncompletePartsUploaded() {
            return incompletePartsUploaded;
        }

        @Override
        public String startMultiPartUpload(String key) throws IOException {
            return createMPUploadId(key);
        }

        @Override
        public UploadPartResponse uploadPart(
                String key, String uploadId, int partNumber, File inputFile, long length)
                throws IOException {
            final byte[] content =
                    getFileContentBytes(inputFile, MathUtils.checkedDownCast(length));
            return storeAndGetUploadPartResult(key, partNumber, content);
        }

        @Override
        public PutObjectResponse putObject(String key, File inputFile) throws IOException {
            final byte[] content =
                    getFileContentBytes(inputFile, MathUtils.checkedDownCast(inputFile.length()));
            return storeAndGetPutObjectResult(key, content);
        }

        @Override
        public boolean deleteObject(String key) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getObject(String key, File targetLocation) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompleteMultipartUploadResponse commitMultiPartUpload(
                String key,
                String uploadId,
                List<Part> partETags,
                long length,
                AtomicInteger errorCount)
                throws IOException {
            return null;
        }

        @Override
        public S3ObjectMetadata getObjectMetadata(String key) throws IOException {
            throw new UnsupportedOperationException();
        }

        private byte[] getFileContentBytes(File file, int length) throws IOException {
            final byte[] content = new byte[length];
            IOUtils.readFully(new FileInputStream(file), content, 0, length);
            return content;
        }

        private UploadPartResponse storeAndGetUploadPartResult(
                String key, int number, byte[] payload) {
            final UploadPartResponse result = createUploadPartResult(key, number, payload);
            completePartsUploaded.add(result);
            return result;
        }

        private PutObjectResponse storeAndGetPutObjectResult(String key, byte[] payload) {
            final PutObjectResponse result = createPutObjectResult(key, payload);
            incompletePartsUploaded.add(result);
            return result;
        }
    }
}
