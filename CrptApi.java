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

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        rateLimiter = new RateLimiter(timeUnit, requestLimit);
    }

    public String createDocument(Object document, String signature) {
        rateLimiter.acquire();
        ObjectMapper mapper = new ObjectMapper();
        DocumentInfo documentInfo = null;
        try {
            documentInfo = new DocumentInfo(
                    DocumentFormat.MANUAL,
                    mapper.writeValueAsString(document),
                    signature,
                    "LP_INTRODUCE_GOODS"
            );
            HttpResponse<String> response;
            try (HttpClient client = HttpClient.newHttpClient()) {
                response = client.send(HttpRequest.newBuilder()
                        .uri(new URI("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(documentInfo)))
                        .build(), HttpResponse.BodyHandlers.ofString());
            }
            if (response.statusCode() == 200) {
                return response.body();
            }
            throw new RuntimeException("Error creating document");
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private record DocumentInfo(
            DocumentFormat document_format,
            String product_document,
            String signature,
            String type
    ) {}

    enum DocumentFormat {
        MANUAL,
        XML,
        CSV;
    }

    private class RateLimiter {
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final Semaphore semaphore;

        public RateLimiter(TimeUnit timeUnit, int requestLimit) {
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
