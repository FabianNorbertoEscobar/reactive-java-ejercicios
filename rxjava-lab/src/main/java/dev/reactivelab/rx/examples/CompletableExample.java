package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 03: Confirmar una operación sin valor.
 *
 *   acción -> finalización o error
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class CompletableExample {
    private CompletableExample() {}

    public static void main(String[] args) {
        Completable.fromAction(() -> System.out.println("Pedido confirmado"))
                .blockingAwait();
    }
}
