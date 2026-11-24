package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 04: Emitir eventos.
 *
 *   eventos -> suscriptor -> fin
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class ObservableExample {
    private ObservableExample() {}

    public static void main(String[] args) {
        Observable.fromArray("creado", "pagado", "enviado")
                .blockingSubscribe(System.out::println);
    }
}
