package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 21: Operadores de transformación: transform, transformToUni, scan, group, asMap.
 *
 *   transform:                    1 -> 10          (≈ map)
 *   transformToUniAndMerge:       internos en paralelo (≈ flatMap)
 *   transformToUniAndConcatenate: de uno en uno, en orden (≈ concatMap)
 *
 * 1. Se transforma una secuencia pequeña con cada operador.
 * 2. Los retardos de las dos líneas centrales muestran la diferencia entre
 *    merge (rápido primero) y concatenate (orden original).
 * 3. Compara con {@code TransformationOperatorsExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   map: [10, 20, 30]
 *   flatMap: [1, 100, 2, 200, 3, 300]
 *   flatMap con retardos: [2, 1]
 *   concatMap con retardos: [1, 2]
 *   scan: [1, 3, 6, 10]
 *   buffer: [[1, 2], [3, 4], [5]]
 *   toMap: {1=uno, 2=dos}
 */
public final class TransformationOperatorsExample {
    private TransformationOperatorsExample() {}

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("map: " + list(Multi.createFrom().items(1, 2, 3).onItem().transform(n -> n * 10)));
        // transformToMultiAndMerge es el equivalente de flatMap.
        log.add("flatMap: " + list(Multi.createFrom().items(1, 2, 3).onItem()
                .transformToMultiAndMerge(n -> Multi.createFrom().items(n, n * 100))));
        log.add("flatMap con retardos: " + list(Multi.createFrom().items(1, 2).onItem()
                .transformToUniAndMerge(TransformationOperatorsExample::slowFirst)));
        log.add("concatMap con retardos: " + list(Multi.createFrom().items(1, 2).onItem()
                .transformToUniAndConcatenate(TransformationOperatorsExample::slowFirst)));
        log.add("scan: " + list(Multi.createFrom().items(1, 2, 3, 4).onItem().scan(Integer::sum)));
        log.add("buffer: " + list(Multi.createFrom().items(1, 2, 3, 4, 5).group().intoLists().of(2)));
        log.add("toMap: " + Multi.createFrom().items(1, 2).collect()
                .asMap(n -> n, n -> n == 1 ? "uno" : "dos").await().indefinitely());
        return log;
    }

    /** El elemento 1 tarda más que el 2, para hacer visible el orden. */
    private static Uni<Integer> slowFirst(int n) {
        return Uni.createFrom().item(n).onItem().delayIt().by(Duration.ofMillis(n == 1 ? 100 : 10));
    }

    private static <T> List<T> list(Multi<T> multi) {
        return multi.collect().asList().await().indefinitely();
    }

    public static void main(String[] args) {
        ExampleRuntime.print(run());
    }
}
