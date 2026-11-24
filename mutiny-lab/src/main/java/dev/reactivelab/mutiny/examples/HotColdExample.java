package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.*;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;

/**
 * 16: Comparar fuentes frías y calientes.
 *
 *   fría: cada suscriptor reinicia
 *   caliente: tardío solo ve eventos futuros
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class HotColdExample {
    private HotColdExample() {}

    public static void main(String[] args) {
        var cold = Multi.createFrom().deferred(() -> Multi.createFrom().items("inicio", "fin"));
        cold.subscribe().with(item -> System.out.println("frío A: " + item));
        cold.subscribe().with(item -> System.out.println("frío B: " + item));
        var hot = BroadcastProcessor.<String>create();
        hot.subscribe().with(item -> System.out.println("caliente A: " + item));
        hot.onNext("inicio");
        hot.subscribe().with(item -> System.out.println("caliente B: " + item));
        hot.onNext("fin");
        hot.onComplete();
    }
}
