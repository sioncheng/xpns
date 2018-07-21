package com.github.sioncheng.xpns.server.akka;

import akka.NotUsed;
import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import akka.pattern.Patterns;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.config.AppProperties;
import org.apache.commons.lang3.StringUtils;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import akka.http.javadsl.marshallers.jackson.Jackson;

import static akka.http.javadsl.server.Directives.*;


public class ApiActor extends AbstractActor {


    public static Props props(String apiHost, int apiPort, ActorRef clientServer) {
        return Props.create(ApiActor.class, ()->new ApiActor(apiHost, apiPort, clientServer));
    }

    ApiActor(String apiHost, int apiPort, ActorRef clientServer) {
        this.apiHost = apiHost;
        this.apiPort = apiPort;
        this.clientServer = clientServer;
    }

    @Override
    public void preStart() throws Exception {

        startSessionActor();

        startHttpServer();

        super.preStart();
    }

    @Override
    public void postStop() throws Exception {
        bind.thenCompose(ServerBinding::unbind);
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(Object.class, x -> {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("received %s", x));
            }
        }).build();
    }

    private void startSessionActor() {
        Props props = SessionActor.props(AppProperties.getString("redis.host"),
                AppProperties.getInt("redis.port"));
        sessionActor = getContext().actorOf(props);
    }

    private void startHttpServer() {
        logger = Logging.getLogger(getContext().getSystem(), this);

        ActorMaterializer actorMaterializer = ActorMaterializer.create(getContext());

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = createRoute().flow(getContext().getSystem(), actorMaterializer);

        http = Http.createExtension((ExtendedActorSystem) getContext().getSystem());
        bind = http.bindAndHandle(routeFlow,
                ConnectHttp.toHost(apiHost, apiPort),
                actorMaterializer);
    }

    private Route createRoute() {
        return route(
                path("client", () -> handleClient()),
                path("notification", ()->handleNotification()),
                path("", () -> handle404())
        );
    }

    private Route handleClient() {
        return route(
            post(()-> entity(FastjsonUnmarshller.unmarshaller(), getClient -> {
                CompletionStage<JSONObject> sessionInfoCompletionStage =
                        PatternsCS.ask(sessionActor, SessionEvent.createAsk(getClient.getString("acid")), 50)
                                .thenApply(x -> {
                                    try {
                                        SessionInfo sessionInfo = (SessionInfo)x;

                                        JSONObject jsonObject = new JSONObject();
                                        if (sessionInfo.getServer().equals("")) {
                                            jsonObject.put("result", "error");
                                        } else {
                                            jsonObject.put("result", "ok");
                                        }
                                        jsonObject.put("sessionInfo", sessionInfo);

                                        return jsonObject;
                                    } catch (Exception ex) {
                                        logger.warning("/client error", ex);

                                        JSONObject jsonObject = new JSONObject();
                                        jsonObject.put("result", "error");
                                        jsonObject.put("desc", ex.getMessage());

                                        return jsonObject;
                                    }
                                });

                return completeOKWithFuture(sessionInfoCompletionStage, Jackson.marshaller()); })
            )
        );
    }

    private Route handleNotification() {
        return route(
            post(()-> entity(FastjsonUnmarshller.unmarshaller(Notification.class), request -> {
                System.out.println(request.getTo());
                clientServer.tell(request, getSelf());
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("result", "ok");
                return complete(StatusCodes.ACCEPTED, jsonObject.toJSONString());
            }))
        );
    }

    private Route handle404() {
        return route(
                get(()-> complete(StatusCodes.ACCEPTED, "404")),
                post(() -> complete(StatusCodes.ACCEPTED, "404"))
        );
    }

    private String apiHost;
    private int apiPort;
    private ActorRef clientServer;
    private ActorRef sessionActor;

    private Http http;
    private CompletionStage<ServerBinding> bind;
    private LoggingAdapter logger;
}
