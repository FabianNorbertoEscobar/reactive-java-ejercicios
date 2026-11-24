package dev.reactivelab.rx;

import dev.reactivelab.domain.FakeRepository;
import dev.reactivelab.domain.Summary;
import io.reactivex.rxjava3.core.Observable;
import java.math.BigDecimal;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class OrderSummaryExampleTest {
    private final OrderSummaryExample example = new OrderSummaryExample(new FakeRepository());

    @Test
    void resumeElPedidoConTestObserver() {
        example.summarize(1).test().awaitDone(5, TimeUnit.SECONDS)
                .assertComplete().assertNoErrors()
                .assertValue(new Summary("Ana", List.of("Teclado", "Ratón"),
                        new BigDecimal("60.00"), "CONFIRMADO"));
    }

    @Test
    void usuarioDesconocidoPropagaError() {
        example.summarize(999).test().awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(NoSuchElementException.class);
    }

    @Test
    void mapYFilterProducenLosValoresEsperados() {
        Observable.just(1, 2, 3).filter(n -> n > 1).map(n -> n * 10)
                .test().assertResult(20, 30);
    }
}
