package io.akka.crawlee.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpResponses;
import io.akka.crawlee.application.CrawlQueueEntity;
import io.akka.crawlee.domain.CrawlQueueState;
import java.util.Optional;

/** The crawl queue's own HTTP surface — enqueue a URL, pull the next dispatchable one, and
 * report back what happened to it. One queue per crawl run, addressed by {@code runId}. */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/runs")
public class CrawlQueueEndpoint {

  private final ComponentClient componentClient;

  public CrawlQueueEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post("/{runId}/requests")
  public HttpResponse enqueue(String runId, CrawlQueueEntity.EnqueueCommand body) {
    var requestId = componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::enqueue).invoke(body);
    return HttpResponses.created(requestId);
  }

  @Post("/{runId}/dispatch")
  public Optional<CrawlQueueEntity.DispatchedRequest> fetchNext(String runId) {
    return componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::fetchNext).invoke();
  }

  @Post("/{runId}/requests/{requestId}/succeeded")
  public HttpResponse reportSuccess(String runId, String requestId) {
    componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::reportSuccess).invoke(requestId);
    return HttpResponses.ok();
  }

  @Post("/{runId}/requests/failed")
  public HttpResponse reportFailure(String runId, CrawlQueueEntity.FailureReport body) {
    componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::reportFailure).invoke(body);
    return HttpResponses.ok();
  }

  @Post("/{runId}/crawl-delay")
  public HttpResponse declareCrawlDelay(String runId, CrawlQueueEntity.DeclareCrawlDelay body) {
    componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::declareCrawlDelay).invoke(body);
    return HttpResponses.ok();
  }

  @Get("/{runId}")
  public CrawlQueueState get(String runId) {
    return componentClient.forEventSourcedEntity(runId).method(CrawlQueueEntity::get).invoke();
  }
}
