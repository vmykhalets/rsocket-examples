import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ServerStreamingToClient {

    public static void main(String[] args) throws IOException, InterruptedException {

        var eventsBroker = new EventsBroker();
        final ObjectMapper objectMapper = new ObjectMapper();

        SocketAcceptor acceptor =
                SocketAcceptor.forRequestStream(
                        payload -> getEvents(payload.getDataUtf8(), eventsBroker).map(eventDto -> {

                            String serialized = null;
                            try {
                                serialized = objectMapper.writeValueAsString(eventDto);
                            } catch (JsonProcessingException e) {
                                log.error("Exception caught", e);
                            }

                            if (serialized != null) {
                                return DefaultPayload.create(serialized);
                            }

                            return null;
                        })
                );

        RSocketServer.create(acceptor).bindNow(TcpServerTransport.create("localhost", 7000));

        // client 1
        RSocket socket1 =
                RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7000)).block();

        var disposable1 = socket1
                .requestStream(DefaultPayload.create("1"))
                .map(Payload::getDataUtf8)
                .doOnNext(p -> log.info("Client 1: " + p))
                .subscribe();


        // client 2
        RSocket socket2 =
                RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7000)).block();

        var disposable2 = socket2
                .requestStream(DefaultPayload.create("2"))
                .map(Payload::getDataUtf8)
                .doOnNext(p -> log.info("Client 2: " + p))
                .subscribe();

        // console reader
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Enter client id: ");
        String id = br.readLine();

        System.out.println("Enter text: ");
        String text = br.readLine();

        while (id.trim().length() > 0 && text.trim().length() > 0) {

            // send event to client
            eventsBroker.sendEvent(id, EventDto.builder().date(new Date()).text(text).build());

            System.out.println("Enter client id: ");
            id = br.readLine();
            if (id.trim().length() == 0) {
                break;
            }

            System.out.println("Enter text: ");
            text = br.readLine();
            if (text.trim().length() == 0) {
                break;
            }
        }

        disposable1.dispose();
        disposable2.dispose();

        Thread.sleep(1000);
    }

    private static Flux<EventDto> getEvents(String id, EventsBroker eventsBroker) {

        ValueHolder<EventListener> valueHolder = new ValueHolder<>(null);

        Flux<EventDto> flux = Flux.create(sink -> {
            EventListener listener = sink::next;
            valueHolder.setValue(listener);

            eventsBroker.addListener(id, listener);
        });

        return flux
                .doOnError(error -> {
                    log.error(error.getMessage());
                })
                .doFinally((signalType) -> {
            log.info("Events stream finished for id: {}", id);
            eventsBroker.removeListener(id, valueHolder.getValue());
        });
    }

    public interface EventListener {
        void onEvent(EventDto dto);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventDto {

        private Date date;
        private String text;

        @Override
        public String toString() {
            return "EventDto{" +
                    "date=" + date.toInstant() +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    public static class EventsBroker {

        private final Map<String, List<EventListener>> listeners = new ConcurrentHashMap<>();

        public void sendEvent(String id, EventDto event) {

            listeners.getOrDefault(id, Collections.emptyList())
                    .forEach(listener -> listener.onEvent(event));
        }

        public void addListener(String id, EventListener eventListener) {

            listeners.compute(id, (key, list) -> {
                if (list == null) {
                    list = Collections.synchronizedList(new ArrayList<>());
                }
                list.add(eventListener);
                return list;
            });
        }

        public void removeListener(String id, EventListener eventListener) {

            listeners.computeIfPresent(id, (key, list) -> {
                list.remove(eventListener);
                return list.isEmpty() ? null : list;
            });
        }
    }

    public static class ValueHolder<T> {

        private T value;

        public ValueHolder(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }
}
