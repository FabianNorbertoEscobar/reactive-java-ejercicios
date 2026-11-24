package dev.reactivelab.rx;

import dev.reactivelab.domain.FakeRepository;
import dev.reactivelab.domain.Order;
import dev.reactivelab.domain.Summary;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.NoSuchElementException;

/**
 * Caso completo: usuario -> pedidos -> productos y estados -> resumen.
 *
 * La base de datos y la API son simuladas y síncronas; fromCallable difiere el
 * trabajo hasta la suscripción y subscribeOn lo ejecuta en el pool de I/O.
 * Las consultas de productos y estados se combinan sin bloquear el pipeline.
 */
public final class OrderSummaryExample {
    private final FakeRepository repository;

    public OrderSummaryExample(FakeRepository repository) {
        this.repository = repository;
    }

    public Single<Summary> summarize(long userId) {
        return Single.fromCallable(() -> repository.findUser(userId)
                        .orElseThrow(() -> new NoSuchElementException("Usuario no encontrado: " + userId)))
                .subscribeOn(Schedulers.io())
                .flatMap(user -> Single.fromCallable(() -> repository.findOrders(user.id()))
                        .subscribeOn(Schedulers.io())
                        .flatMap(orders -> {
                            var products = Observable.fromIterable(orders)
                                    .flatMapIterable(Order::productIds)
                                    .concatMapSingle(id -> Single.fromCallable(() -> repository.findProduct(id))
                                            .subscribeOn(Schedulers.io()))
                                    .toList();
                            var statuses = Observable.fromIterable(orders)
                                    .concatMapSingle(order -> Single.fromCallable(() -> repository.fetchStatus(order.id()))
                                            .subscribeOn(Schedulers.io()))
                                    .toList();
                            return Single.zip(products, statuses, (items, states) ->
                                    Summary.of(user, items, states.isEmpty() ? "SIN_PEDIDOS" : String.join(", ", states)));
                        }));
    }

    public static void main(String[] args) {
        System.out.println(new OrderSummaryExample(new FakeRepository()).summarize(1).blockingGet());
    }
}
