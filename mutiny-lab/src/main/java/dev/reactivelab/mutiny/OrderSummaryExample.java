package dev.reactivelab.mutiny;

import dev.reactivelab.domain.FakeRepository;
import dev.reactivelab.domain.Order;
import dev.reactivelab.domain.Summary;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;

/**
 * Mismo caso que RxJava: usuario -> pedidos -> productos y estados -> resumen.
 *
 * La base de datos y la API son simuladas y síncronas; los proveedores se
 * evalúan al suscribirse y runSubscriptionOn los desplaza a otro pool.
 * La combinación no bloquea el pipeline; await solo se utiliza en main.
 */
public final class OrderSummaryExample {
    private final FakeRepository repository;

    public OrderSummaryExample(FakeRepository repository) {
        this.repository = repository;
    }

    public Uni<Summary> summarize(long userId) {
        return Uni.createFrom().item(() -> repository.findUser(userId)
                        .orElseThrow(() -> new NoSuchElementException("Usuario no encontrado: " + userId)))
                .runSubscriptionOn(ForkJoinPool.commonPool())
                .onItem().transformToUni(user -> Uni.createFrom().item(() -> repository.findOrders(user.id()))
                        .runSubscriptionOn(ForkJoinPool.commonPool())
                        .onItem().transformToUni(orders -> {
                            var products = Multi.createFrom().iterable(orders)
                                    .onItem().transformToMultiAndConcatenate(order ->
                                            Multi.createFrom().iterable(order.productIds()))
                                    .onItem().transformToUniAndConcatenate(id ->
                                            Uni.createFrom().item(() -> repository.findProduct(id))
                                                    .runSubscriptionOn(ForkJoinPool.commonPool()))
                                    .collect().asList();
                            var statuses = Multi.createFrom().iterable(orders)
                                    .onItem().transformToUniAndConcatenate(order ->
                                            Uni.createFrom().item(() -> repository.fetchStatus(order.id()))
                                                    .runSubscriptionOn(ForkJoinPool.commonPool()))
                                    .collect().asList();
                            return Uni.combine().all().unis(products, statuses).asTuple()
                                    .map(tuple -> Summary.of(user, tuple.getItem1(), tuple.getItem2().isEmpty()
                                            ? "SIN_PEDIDOS" : String.join(", ", tuple.getItem2())));
                        }));
    }

    public static void main(String[] args) {
        System.out.println(new OrderSummaryExample(new FakeRepository()).summarize(1).await().indefinitely());
    }
}
