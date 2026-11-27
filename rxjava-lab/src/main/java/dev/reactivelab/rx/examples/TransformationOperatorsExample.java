package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 21: Operadores de transformación: map, flatMap, concatMap, scan, buffer, toMap.
 *
 *   map:        1 -> 10
 *   flatMap:    1 -> [1, 100]     (internos en paralelo, el orden puede cambiar)
 *   concatMap:  1 -> [1, 100]     (internos de uno en uno, conserva el orden)
 *
 * 1. Se transforma una secuencia pequeña con cada operador.
 * 2. Los retardos de las dos últimas líneas muestran la diferencia entre
 *    flatMap (rápido primero) y concatMap (orden original).
 * 3. Compara con {@code TransformationOperatorsExample} del módulo Mutiny.
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
        log.add("map: " + Observable.just(1, 2, 3).map(n -> n * 10).toList().blockingGet());
        log.add("flatMap: " + Observable.just(1, 2, 3)
                .flatMap(n -> Observable.just(n, n * 100)).toList().blockingGet());
        log.add("flatMap con retardos: " + Observable.just(1, 2)
                .flatMap(TransformationOperatorsExample::slowFirst).toList().blockingGet());
        log.add("concatMap con retardos: " + Observable.just(1, 2)
                .concatMap(TransformationOperatorsExample::slowFirst).toList().blockingGet());
        // scan emite cada acumulado parcial; reduce solo emitiría el final.
        log.add("scan: " + Observable.just(1, 2, 3, 4).scan(Integer::sum).toList().blockingGet());
        log.add("buffer: " + Observable.just(1, 2, 3, 4, 5).buffer(2).toList().blockingGet());
        log.add("toMap: " + Observable.just(1, 2)
                .toMap(n -> n, n -> n == 1 ? "uno" : "dos").blockingGet());
        return log;
    }

    /** El elemento 1 tarda más que el 2, para hacer visible el orden. */
    private static Observable<Integer> slowFirst(int n) {
        return Observable.just(n).delay(n == 1 ? 100 : 10, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
