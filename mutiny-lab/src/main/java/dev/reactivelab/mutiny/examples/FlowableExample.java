package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.*;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

/**
 * 05: Solicitar dos eventos.
 *
 *   fuente -- request(2) --> dos eventos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class FlowableExample {
    private FlowableExample() {}

    public static void main(String[] args) {
        var subscriber = Multi.createFrom().range(1, 6).subscribe()
                .withSubscriber(AssertSubscriber.create(0));
        subscriber.request(2);
        System.out.println(subscriber.getItems());
        subscriber.cancel();
    }
}
