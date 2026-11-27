package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import java.util.concurrent.ExecutorService;
import java.util.List;

/** Utilidad para los {@code main}: imprime el resultado y libera los hilos por defecto de Mutiny. */
final class ExampleRuntime {
    private ExampleRuntime() {}

    /**
     * Mutiny crea hilos no daemon al usar retardos o reintentos; sin cerrarlos la JVM no termina.
     * Solo es necesario en programas de consola, no en pruebas ni en aplicaciones.
     */
    static void print(List<String> lines) {
        lines.forEach(System.out::println);
        ((ExecutorService) Infrastructure.getDefaultExecutor()).shutdownNow();
        Infrastructure.getDefaultWorkerPool().shutdownNow();
    }
}
