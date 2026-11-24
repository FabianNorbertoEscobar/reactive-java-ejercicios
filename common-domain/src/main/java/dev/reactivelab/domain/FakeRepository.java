package dev.reactivelab.domain;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Simula consultas síncronas; las bibliotecas adaptan sus resultados al suscribirse. */
public final class FakeRepository {
    public Optional<User> findUser(long id) {
        return FakeDatabase.USERS.stream().filter(user -> user.id() == id).findFirst();
    }

    public List<Order> findOrders(long userId) {
        return FakeDatabase.ORDERS.stream().filter(order -> order.userId() == userId).toList();
    }

    public Product findProduct(long id) {
        return FakeDatabase.PRODUCTS.stream().filter(product -> product.id() == id).findFirst()
                .orElseThrow(() -> new NoSuchElementException("Producto no encontrado: " + id));
    }

    /** Simula una API externa y permite comprobar la propagación de errores. */
    public String fetchStatus(long orderId) {
        if (FakeDatabase.ORDERS.stream().noneMatch(order -> order.id() == orderId)) {
            throw new NoSuchElementException("Pedido no encontrado: " + orderId);
        }
        return "CONFIRMADO";
    }
}
