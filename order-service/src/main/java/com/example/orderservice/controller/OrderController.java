package com.example.orderservice.controller;

import com.example.basedomains.dto.Order;
import com.example.basedomains.dto.OrderEvent;
import com.example.orderservice.kafka.OrderKafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class OrderController {
    private OrderKafkaProducer orderKafkaProducer;

    @Autowired
    public OrderController(OrderKafkaProducer orderKafkaProducer){
        this.orderKafkaProducer = orderKafkaProducer;
    }

    //http://localhost:8080/api/v1/orders
    @PostMapping("/orders")
    public String placeOrder(@RequestBody Order order){
        order.setOrderId(UUID.randomUUID().toString());
        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setStatus("PENDING");
        orderEvent.setMessage("Order status is in pending state");
        orderEvent.setOrder(order);

        orderKafkaProducer.sendMessage(orderEvent);
        return "Order placed successfully";
    }

}
