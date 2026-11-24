package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 18: Obtener productos de un pedido.
 *
 *   pedido -> flujo de ids -> productos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class TransformToMultiExample {
    private TransformToMultiExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Single.fromCallable(() -> repo.findOrders(1).getFirst())
                .flattenAsObservable(Order::productIds).map(repo::findProduct)
                .blockingSubscribe(System.out::println);
    }
}
