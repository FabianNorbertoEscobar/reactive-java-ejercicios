package dev.reactivelab.domain;

import java.math.BigDecimal;
import java.util.List;

/** Datos deterministas en memoria; no abre conexiones ni hace I/O real. */
public final class FakeDatabase {
    private FakeDatabase() {}

    public static final List<User> USERS = List.of(new User(1, "Ana"), new User(2, "Luis"));
    public static final List<Product> PRODUCTS = List.of(
            new Product(10, "Teclado", new BigDecimal("40.00")),
            new Product(20, "Ratón", new BigDecimal("20.00")));
    public static final List<Order> ORDERS = List.of(
            new Order(100, 1, List.of(10L, 20L)),
            new Order(101, 2, List.of(20L)));
}
