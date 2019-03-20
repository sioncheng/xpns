package com.github.sioncheng.xpns.server.akka;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;

import akka.actor.AbstractActor;
import akka.io.TcpMessage;
import akka.kafka.ProducerSettings;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.config.AppProperties;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ServerActor extends AbstractActor {

    public static Props props() {
        return Props.create(ServerActor.class,() -> new ServerActor());
    }

    public ServerActor () {
        this.clients = new HashMap<>();
        this.logger = Logging.getLogger(getContext().getSystem(), this);
    }

    @Override
    public void preStart() throws Exception {
        startSessionActor();
        startKafkaProducer();
        super.preStart();
    }

    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(Start.class, msg -> startTcpServer())
                .match(Tcp.Bound.class, msg -> {
                    logger.info("bound");
                })
                .match(Tcp.Connected.class, msg -> {
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("connected %s %s", msg.localAddress(), msg.remoteAddress()));
                    }
                    Props clientActorProps = ClientActor.props(getSelf(), getSender());
                    final ActorRef clientActor = getContext().getSystem().actorOf(clientActorProps);
                    getSender().tell(TcpMessage.register(clientActor), getSelf());
                })
                .match(ClientActivation.class, this::handleClientActivation)
                .match(Notification.class, this::handelNotificationRequest)
                .build();
    }

    private void startSessionActor() {
        Props props = SessionActor.props(AppProperties.getString("redis.host"),
                AppProperties.getInt("redis.port"));
        sessionActor = getContext().actorOf(props);
    }

    private void startKafkaProducer() {
        Map<String, String> appSettings = AppProperties.getPropertiesWithPrefix("kafka.producer.");
        Config config = getContext().getSystem().settings().config().getConfig("akka.kafka.producer");
        ProducerSettings<String, String> settings = ProducerSettings.create(config,
                new StringSerializer(),
                new StringSerializer()).withBootstrapServers(appSettings.get("bootstrap.servers"));
        producer = settings.createKafkaProducer();
    }

    private void startTcpServer() {

        tcpManager = Tcp.createExtension((ExtendedActorSystem)getContext().getSystem()).manager();

        tcpManager.tell(TcpMessage.bind(getSelf(),
                new InetSocketAddress("0.0.0.0", AppProperties.getInt("server.port")),
                100),
                getSelf());

        getContext().getSystem().scheduler().schedule(Duration.ofMinutes(10),
                Duration.ofMinutes(10),
                ()->{
                    Set<Map.Entry<String, ClientActivation>> set = clients.entrySet();
                    for (Map.Entry<String, ClientActivation> kv :
                            set) {
                        long ts = System.currentTimeMillis() - kv.getValue().time();
                        if (ts > MILLIS_OF_10_MINUTE) {
                            closeClient(kv.getKey(), kv.getValue().client());
                        }
                    }
                },
                getContext().dispatcher());
    }

    private void handleClientActivation(ClientActivation activation) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("client activation %s %d", activation.acid(), activation.status()));
        }

        if (StringUtils.isEmpty(activation.acid())) {
            return;
        }

        ClientActivation activation1 = clients.get(activation.acid());
        switch (activation.status()) {
            case ClientActivation.LOGON:
                if (activation1 != null) {
                    closeClient(activation.acid(), activation1.client());
                }

                clients.put(activation.acid(), activation);

                SessionEvent sessionEvent = SessionEvent.create(activation.acid(),
                        SessionEvent.LOGON,
                        AppProperties.getString("server.api.host"),
                        AppProperties.getInt("server.api.port"));
                sessionActor.tell(sessionEvent, getSelf());

                //
                SessionInfo sessionInfo = new SessionInfo();
                sessionInfo.setAcid(activation.acid());
                sessionInfo.setServer(AppProperties.getString("server.api.host"));
                sessionInfo.setPort(AppProperties.getInt("server.api.port"));

                ProducerRecord<String, String> logonEvent =
                        new ProducerRecord<>(AppProperties.getString("kafka-logon-topic"),
                                activation.acid(),
                                JSON.toJSONString(sessionInfo));
                producer.send(logonEvent);

                break;
            case ClientActivation.LOGOUT:
                if (activation1 != null && activation1.version().equalsIgnoreCase(activation.version())) {
                    clients.remove(activation.acid());
                    closeClient(activation1.acid(), activation1.client());
                } else {
                    //log warning
                    logger.warning(String.format("unable to remove client actor for %s", activation.acid()));
                }

                SessionEvent sessionEventLogout = SessionEvent.create(activation.acid(),
                        SessionEvent.LOGOUT,
                        AppProperties.getString("server.api.host"),
                        AppProperties.getInt("server.api.port"));
                sessionActor.tell(sessionEventLogout, getSelf());

                break;
            case ClientActivation.HEARTBEAT:
                if (activation1 != null && activation1.version().equalsIgnoreCase(activation.version())) {
                    clients.put(activation.acid(), activation);
                } else {
                    //log warning
                    logger.warning(String.format("no client actor for %s during heart beat", activation.acid()));
                }

                SessionEvent sessionEventHeartbeat = SessionEvent.create(activation.acid(),
                        SessionEvent.HEARTBEAT,
                        AppProperties.getString("server.api.host"),
                        AppProperties.getInt("server.api.port"));
                sessionActor.tell(sessionEventHeartbeat, getSelf());

                break;
            case ClientActivation.ACK:
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("notification ack %s", activation.jsonObject().toJSONString()));
                }
                JSONObject jsonObject = activation.jsonObject();
                Notification notification = jsonObject.getObject("notification", Notification.class);
                NotificationEntity notificationEntity = new NotificationEntity();
                notificationEntity.setStatus(NotificationEntity.DELIVERED);
                notificationEntity.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));
                notificationEntity.setCreateDateTime(notificationEntity.getStatusDateTime());
                notificationEntity.setTtl(0);
                notificationEntity.setNotification(notification);

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(AppProperties.getString("kafka-ack-topic"),
                        notification.getTo(),
                        JSON.toJSONString(notificationEntity));
                producer.send(producerRecord);
                break;
        }
    }

    private void handelNotificationRequest(Notification request) {
        ClientActivation activation = clients.get(request.getTo());
        if (activation == null) {
            logger.warning(String.format("cant find target client for %s", request.getTo()));
        } else {
            activation.client().tell(request, getSelf());
        }
    }

    private void closeClient(String acid, ActorRef client) {
        getContext().stop(client);
    }

    private ActorRef tcpManager;

    private ActorRef sessionActor;

    private KafkaProducer<String, String> producer;

    private HashMap<String, ClientActivation> clients;

    private LoggingAdapter logger;

    private static final long MILLIS_OF_10_MINUTE = 10 * 60 * 1000L;

    public final static class Start{}
}
