package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.*;

/**
 * 10: Mezclar eventos.
 *
 *   fuente A --+--> eventos intercalados
 *   fuente B --+
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MergeExample {
    private MergeExample() {}

    public static void main(String[] args) {
        Multi.createBy().merging().streams(Multi.createFrom().items("A1", "A2"),
                Multi.createFrom().items("B1", "B2"))
                .collect().asList().await().indefinitely().forEach(System.out::println);
    }
}
