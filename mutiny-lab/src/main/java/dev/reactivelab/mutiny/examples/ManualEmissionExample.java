package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 26: Emisión manual de eventos: Multi.createFrom().emitter(), MultiEmitter, Uni emitter,
 *     generator y UnicastProcessor.
 *
 *   callback/listener --emitter.emit--> Multi --> suscriptor
 *
 * Diferencia clave con un Subject/BroadcastProcessor: {@code emitter} es FRÍO. El lambda se
 * ejecuta de nuevo por cada suscriptor y cada uno recibe su propia secuencia. Un procesador
 * es CALIENTE y comparte una única emisión entre todos.
 *
 * 1. Dos suscriptores a un Multi con emitter: el productor se ejecuta dos veces.
 * 2. {@code onTermination} libera el recurso cuando se cancela o termina.
 * 3. {@code UnicastProcessor} acumula eventos hasta que llega su único suscriptor.
 * 4. Compara con {@code ManualEmissionExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   emitter frío: ejecuciones=2 A=[1, 2] B=[1, 2]
 *   cancelación: [1] liberado=true
 *   emitter con buffer: [1, 2, 3]
 *   Uni emitter: resultado
 *   generator: [1, 2, 3, 4]
 *   UnicastProcessor: [a, b]
 */
public final class ManualEmissionExample {
    private ManualEmissionExample() {}

    public static String coldEmitter() {
        var executions = new AtomicInteger();
        var source = Multi.createFrom().<Integer>emitter(emitter -> {
            executions.incrementAndGet();
            emitter.emit(1);
            emitter.emit(2);
            emitter.complete();
        });
        var a = source.collect().asList().await().indefinitely();
        var b = source.collect().asList().await().indefinitely();
        return "ejecuciones=" + executions.get() + " A=" + a + " B=" + b;
    }

    public static String cancellation() {
        var released = new AtomicInteger();
        var received = new ArrayList<Integer>();
        Multi.createFrom().<Integer>emitter(emitter -> {
            emitter.onTermination(released::incrementAndGet); // se invoca al cancelar o terminar
            emitter.emit(1);
            // No se completa: simula un listener abierto hasta que el consumidor se va.
        }).subscribe().with(received::add).cancel();
        return received + " liberado=" + (released.get() == 1);
    }

    /** El emitter respeta la demanda mediante una estrategia de overflow (aquí, un buffer). */
    public static List<Integer> emitterWithBuffer() {
        return Multi.createFrom().<Integer>emitter(emitter -> {
            for (int i = 1; i <= 3 && !emitter.isCancelled(); i++) emitter.emit(i);
            emitter.complete();
        }).onOverflow().buffer(16).collect().asList().await().indefinitely();
    }

    public static String uniEmitter() {
        return Uni.createFrom().<String>emitter(emitter -> emitter.complete("resultado"))
                .await().indefinitely();
    }

    /** generator emite un elemento por llamada y solo cuando hay demanda (≈ Flowable.generate). */
    public static List<Integer> generator() {
        return Multi.createFrom().<Integer, Integer>generator(() -> 1, (state, emitter) -> {
            emitter.emit(state);
            if (state == 4) emitter.complete();
            return state + 1;
        }).collect().asList().await().indefinitely();
    }

    public static List<String> unicast() {
        var processor = UnicastProcessor.<String>create();
        processor.onNext("a"); // se acumula: todavía no hay suscriptor
        processor.onNext("b");
        processor.onComplete();
        return processor.collect().asList().await().indefinitely();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("emitter frío: " + coldEmitter());
        log.add("cancelación: " + cancellation());
        log.add("emitter con buffer: " + emitterWithBuffer());
        log.add("Uni emitter: " + uniEmitter());
        log.add("generator: " + generator());
        log.add("UnicastProcessor: " + unicast());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
