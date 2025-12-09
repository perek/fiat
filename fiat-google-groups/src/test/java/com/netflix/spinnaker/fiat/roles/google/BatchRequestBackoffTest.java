/*
 * Copyright 2024 Snap Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.fiat.roles.google;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.ExponentialBackOff;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test to verify whether HttpBackOffUnsuccessfulResponseHandler works with BatchRequest. This tests
 * the behavior seen in GoogleDirectoryUserRolesProvider.multiLoadRoles()
 */
public class BatchRequestBackoffTest {

  private AtomicInteger requestCount;
  private AtomicInteger backoffHandlerInvocations;
  private AtomicInteger callbackFailures;

  @BeforeEach
  public void setUp() {
    requestCount = new AtomicInteger(0);
    backoffHandlerInvocations = new AtomicInteger(0);
    callbackFailures = new AtomicInteger(0);
  }

  @Test
  public void testBackoffHandlerWithBatchRequest() throws IOException {
    System.out.println(
        "Testing whether HttpBackOffUnsuccessfulResponseHandler works with BatchRequest...\n");

    // Create a mock HTTP transport that simulates rate limiting
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                int count = requestCount.incrementAndGet();
                System.out.println("HTTP Request #" + count + " executing...");

                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();

                // First 2 requests: return 403 rate limit error
                // Third request: succeed
                if (count <= 2) {
                  System.out.println("  -> Returning 403 (rate limited)");
                  response.setStatusCode(HttpStatusCodes.STATUS_CODE_FORBIDDEN);
                  response.setContentType(Json.MEDIA_TYPE);
                  response.setContent(
                      "{\"error\": {\"code\": 403, \"message\": \"Request rate higher than configured\"}}");
                } else {
                  System.out.println("  -> Returning 200 (success)");
                  response.setStatusCode(HttpStatusCodes.STATUS_CODE_OK);
                  response.setContentType(Json.MEDIA_TYPE);
                  response.setContent("{}");
                }

                return response;
              }
            };
          }
        };

    // Create a batch request
    BatchRequest batchRequest = new BatchRequest(transport, null);

    // Create an HTTP request with backoff handler
    HttpRequest request =
        transport
            .createRequestFactory()
            .buildGetRequest(new com.google.api.client.http.GenericUrl("https://example.com/test"));

    // Set up the backoff handler - this is what GoogleDirectoryUserRolesProvider does
    HttpBackOffUnsuccessfulResponseHandler handler =
        new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()) {
          @Override
          public boolean handleResponse(
              HttpRequest request, HttpResponse response, boolean supportsRetry)
              throws IOException {
            int invocation = backoffHandlerInvocations.incrementAndGet();
            System.out.println(
                ">>> BACKOFF HANDLER INVOKED (invocation #"
                    + invocation
                    + ")! Response code: "
                    + response.getStatusCode());
            return super.handleResponse(request, response, supportsRetry);
          }
        };

    handler.setBackOffRequired(
        response -> {
          int code = response.getStatusCode();
          boolean shouldBackoff = code == 403 || code / 100 == 5;
          System.out.println(
              ">>> Checking if backoff required for status code " + code + ": " + shouldBackoff);
          return shouldBackoff;
        });

    request.setUnsuccessfulResponseHandler(handler);

    // Queue the request with a callback
    JsonBatchCallback<Object> callback =
        new JsonBatchCallback<Object>() {
          @Override
          public void onSuccess(Object result, HttpHeaders responseHeaders) {
            System.out.println(">>> Batch callback: Success!");
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            int failure = callbackFailures.incrementAndGet();
            System.out.println(">>> Batch callback: Failure #" + failure + " - " + e.getMessage());
          }
        };

    batchRequest.queue(request, Object.class, GoogleJsonErrorContainer.class, callback);

    // Execute the batch
    System.out.println("\nExecuting batch request...\n");

    try {
      batchRequest.execute();
      fail("Expected batch.execute() to throw HttpResponseException due to 403 error");
    } catch (IOException e) {
      System.out.println(
          "Caught exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    // Print results
    System.out.println("\n========================================");
    System.out.println("=== Test Results ===");
    System.out.println("========================================");
    System.out.println("Total HTTP requests made: " + requestCount.get());
    System.out.println("Backoff handler invocations: " + backoffHandlerInvocations.get());
    System.out.println("Callback failures: " + callbackFailures.get());
    System.out.println("========================================\n");

    // Assertions - verify EXPECTED behavior (backoff handler SHOULD be invoked)
    // NOTE: These assertions represent the CORRECT/EXPECTED behavior.
    // Currently they FAIL because the code is buggy - the backoff handler doesn't work with
    // BatchRequest.
    // When someone fixes the bug, these tests will pass.

    assertEquals(
        2,
        backoffHandlerInvocations.get(),
        "Backoff handler SHOULD be invoked for retries (currently fails - bug in code)");
    assertEquals(
        3,
        requestCount.get(),
        "Should retry after 403 errors: initial request + 2 retries = 3 total (currently fails - bug in code)");

    // Log actual vs expected
    System.out.println("\n=== EXPECTED vs ACTUAL ===");
    System.out.println("Expected: Backoff handler invoked 2+ times, 3+ HTTP requests made");
    System.out.println(
        "Actual:   Backoff handler invoked "
            + backoffHandlerInvocations.get()
            + " times, "
            + requestCount.get()
            + " HTTP requests made");

    if (backoffHandlerInvocations.get() == 0) {
      System.out.println("\n❌ BUG CONFIRMED: Backoff handler is NOT working with BatchRequest");
      System.out.println("   The retry logic in GoogleDirectoryUserRolesProvider.multiLoadRoles()");
      System.out.println("   on lines 129-135 is INEFFECTIVE dead code that never executes!");
    } else {
      System.out.println("\n✅ Backoff handler is working correctly!");
    }
  }
}
