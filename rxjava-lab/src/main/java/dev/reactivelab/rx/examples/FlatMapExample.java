package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 07: Buscar pedidos de un usuario.
 *
 *   usuario -> consulta asíncrona -> pedidos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class FlatMapExample {
    private FlatMapExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Single.fromCallable(() -> repo.findUser(1).orElseThrow())
                .flatMap(user -> Single.fromCallable(() -> repo.findOrders(user.id())))
                .blockingGet().forEach(System.out::println);
    }
}
