package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.*;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

/**
 * 15: Consumir solo tres eventos.
 *
 *   fuente -- demanda(3) --> 1, 2, 3 -- cancelar
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class BackpressureExample {
    private BackpressureExample() {}

    public static void main(String[] args) {
        var subscriber = Multi.createFrom().range(1, 101).subscribe()
                .withSubscriber(AssertSubscriber.create(0));
        subscriber.request(3);
        System.out.println(subscriber.getItems());
        subscriber.cancel();
    }
}
