package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

/**
 * 14: Cambiar el hilo de suscripción y de emisión.
 *
 *   consulta -- hilo I/O --> emisión -- otro hilo --> consumidor
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class SchedulersExample {
    private SchedulersExample() {}

    public static void main(String[] args) {
        try (var subscriptionExecutor = Executors.newSingleThreadExecutor()) {
            System.out.println(Uni.createFrom().item(() -> "Fuente: " + Thread.currentThread().getName())
                    .runSubscriptionOn(subscriptionExecutor)
                    .emitOn(ForkJoinPool.commonPool())
                    .map(source -> source + ", consumidor: " + Thread.currentThread().getName())
                    .await().indefinitely());
        }
    }
}
