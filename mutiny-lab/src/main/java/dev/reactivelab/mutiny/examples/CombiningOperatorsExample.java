package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 23: Operadores de combinación: merging, concatenating, combining (zip y latestItems).
 *
 *   merging:      A ---A1----   concatenating: A1 luego B1 (espera a que A termine)
 *                 B -B1------   combining().streams(..).using: empareja por posición
 *   latestItems:  cada emisión se combina con el último valor de la otra fuente
 *
 * 1. Dos fuentes con retardos distintos muestran el orden de merge y concat.
 * 2. Dos procesadores controlados a mano hacen determinista latestItems.
 * 3. Compara con {@code CombiningOperatorsExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   merge: [B1, A1]
 *   concat: [A1, B1]
 *   zip: [1a, 2b]
 *   combineLatest: [1x, 2x, 2y]
 *   startWith: [0, 1, 2]
 *   Uni zip: Ana: CONFIRMADO
 */
public final class CombiningOperatorsExample {
    private CombiningOperatorsExample() {}

    private static Multi<String> slowA() {
        return Uni.createFrom().item("A1").onItem().delayIt().by(Duration.ofMillis(100)).toMulti();
    }

    private static Multi<String> fastB() {
        return Uni.createFrom().item("B1").onItem().delayIt().by(Duration.ofMillis(10)).toMulti();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("merge: " + list(Multi.createBy().merging().streams(slowA(), fastB())));
        log.add("concat: " + list(Multi.createBy().concatenating().streams(slowA(), fastB())));
        log.add("zip: " + list(Multi.createBy().combining()
                .streams(Multi.createFrom().items(1, 2, 3), Multi.createFrom().items("a", "b"))
                .using((n, s) -> n + s)));

        var numbers = BroadcastProcessor.<Integer>create();
        var letters = BroadcastProcessor.<String>create();
        var combined = new ArrayList<String>();
        Multi.createBy().combining().streams(numbers, letters).latestItems()
                .using((n, s) -> n + s).subscribe().with(combined::add);
        numbers.onNext(1);   // sin letra todavía: no emite
        letters.onNext("x"); // 1x
        numbers.onNext(2);   // 2x
        letters.onNext("y"); // 2y
        log.add("combineLatest: " + combined);

        // Mutiny no tiene startWith: se concatena una fuente con el valor inicial.
        log.add("startWith: " + list(Multi.createBy().concatenating()
                .streams(Multi.createFrom().item(0), Multi.createFrom().items(1, 2))));
        log.add("Uni zip: " + Uni.combine().all().unis(Uni.createFrom().item("Ana"), Uni.createFrom().item("CONFIRMADO"))
                .with((name, status) -> name + ": " + status).await().indefinitely());
        return log;
    }

    private static <T> List<T> list(Multi<T> multi) {
        return multi.collect().asList().await().indefinitely();
    }

    public static void main(String[] args) {
        ExampleRuntime.print(run());
    }
}
