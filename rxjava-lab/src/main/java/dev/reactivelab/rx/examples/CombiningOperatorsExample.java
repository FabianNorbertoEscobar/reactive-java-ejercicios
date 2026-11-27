package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 23: Operadores de combinación: merge, concat, zip, combineLatest y startWith.
 *
 *   merge:  A ---A1----   concat: A1 luego B1 (espera a que A termine)
 *           B -B1------   zip:    empareja por posición (1ª con 1ª, 2ª con 2ª)
 *   combineLatest: cada emisión se combina con el último valor de la otra fuente
 *
 * 1. Dos fuentes con retardos distintos muestran el orden de merge y concat.
 * 2. Dos subjects controlados a mano hacen determinista combineLatest.
 * 3. Compara con {@code CombiningOperatorsExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   merge: [B1, A1]
 *   concat: [A1, B1]
 *   zip: [1a, 2b]
 *   combineLatest: [1x, 2x, 2y]
 *   startWith: [0, 1, 2]
 *   Single.zip: Ana: CONFIRMADO
 */
public final class CombiningOperatorsExample {
    private CombiningOperatorsExample() {}

    private static Observable<String> slowA() {
        return Observable.just("A1").delay(100, TimeUnit.MILLISECONDS);
    }

    private static Observable<String> fastB() {
        return Observable.just("B1").delay(10, TimeUnit.MILLISECONDS);
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("merge: " + Observable.merge(slowA(), fastB()).toList().blockingGet());
        log.add("concat: " + Observable.concat(slowA(), fastB()).toList().blockingGet());
        log.add("zip: " + Observable.zip(Observable.just(1, 2, 3), Observable.just("a", "b"),
                (n, s) -> n + s).toList().blockingGet());

        var numbers = PublishSubject.<Integer>create();
        var letters = PublishSubject.<String>create();
        var combined = new ArrayList<String>();
        Observable.combineLatest(numbers, letters, (n, s) -> n + s).subscribe(combined::add);
        numbers.onNext(1);   // sin letra todavía: no emite
        letters.onNext("x"); // 1x
        numbers.onNext(2);   // 2x
        letters.onNext("y"); // 2y
        log.add("combineLatest: " + combined);

        log.add("startWith: " + Observable.just(1, 2).startWithItem(0).toList().blockingGet());
        log.add("Single.zip: " + Single.zip(Single.just("Ana"), Single.just("CONFIRMADO"),
                (name, status) -> name + ": " + status).blockingGet());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
