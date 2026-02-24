cube(`Orders`, {
  sql: `SELECT * FROM iceberg.warehouse.sales.orders`,

  measures: {
    count: {
      type: `count`,
    },
    totalRevenue: {
      sql: `price * quantity`,
      type: `sum`,
      format: `currency`,
    },
    avgPrice: {
      sql: `price`,
      type: `avg`,
      format: `currency`,
    },
    totalQuantity: {
      sql: `quantity`,
      type: `sum`,
    },
  },

  dimensions: {
    orderId: {
      sql: `order_id`,
      type: `string`,
      primaryKey: true,
    },
    customerId: {
      sql: `customer_id`,
      type: `string`,
    },
    productName: {
      sql: `product_name`,
      type: `string`,
    },
    quantity: {
      sql: `quantity`,
      type: `number`,
    },
    price: {
      sql: `price`,
      type: `number`,
      format: `currency`,
    },
    orderTimestamp: {
      sql: `order_timestamp`,
      type: `time`,
    },
  },

  segments: {
    highValue: {
      sql: `${CUBE}.price >= 100`,
    },
  },
});

