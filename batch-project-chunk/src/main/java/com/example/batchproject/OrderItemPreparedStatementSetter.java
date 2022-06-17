package com.example.batchproject;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OrderItemPreparedStatementSetter implements ItemPreparedStatementSetter<Order> {
    @Override
    public void setValues(Order order, PreparedStatement preparedStatement) throws SQLException {
        System.out.println(order);
        preparedStatement.setInt(1,order.getOrderId());
        preparedStatement.setString(2, order.getFirstName());
        preparedStatement.setString(3, order.getLastName());
        preparedStatement.setString(4, order.getEmail());
        preparedStatement.setString(5, order.getCost().toString());
        preparedStatement.setString(6, order.getItemId());
        preparedStatement.setString(7, order.getItemName());
        preparedStatement.setDate(8, new Date(order.getShipDate().getTime()));
    }
}
