import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;

public class CrptApi {
    private final RateLimiter rateLimiter;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final static URI documentCreateURI;

    static {
        try {
            documentCreateURI = new URI("https://ismp.crpt.ru/api/v3/lk/documents/create");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        rateLimiter = new RateLimiter(timeUnit, requestLimit);
        httpClient = HttpClient.newBuilder().build();
        objectMapper = new ObjectMapper();
    }

    public String createDocument(Object document, String signature) {
        rateLimiter.acquire();
        try {
            DocumentInfo documentInfo = new DocumentInfo(
                    DocumentFormat.MANUAL,
                    objectMapper.writeValueAsString(document),
                    signature,
                    "LP_INTRODUCE_GOODS"
            );
            HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                    .uri(documentCreateURI)
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(documentInfo)))
                    .build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return response.body();
            }
            throw new RuntimeException("Error creating document");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private record DocumentInfo(
            @JsonProperty("document_format")
            DocumentFormat documentFormat,
            @JsonProperty("product_document")
            String productDocument,
            String signature,
            String type
    ) {
    }

    enum DocumentFormat {
        MANUAL,
        XML,
        CSV;
    }

    private class RateLimiter {
        private final ScheduledExecutorService scheduler;
        private final Semaphore semaphore;

        public RateLimiter(TimeUnit timeUnit, int requestLimit) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            semaphore = new Semaphore(requestLimit, true);
            scheduler.scheduleAtFixedRate(() -> {
                int permitsToAdd = requestLimit - semaphore.availablePermits();
                if (permitsToAdd > 0) {
                    semaphore.release(permitsToAdd);
                }
            }, 0, 1, timeUnit);
        }

        public void acquire() {
            semaphore.acquireUninterruptibly();
        }
    }
}
