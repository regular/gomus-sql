module.exports = function(connection) {

  return {
    bookings
  }

  function bookings(from, to) {
    const sql = `
SELECT
  b.id,
  product_id,
  p.name AS product_name,
  l.name AS language_name,
  c.name AS country_name,
  ca.zip AS zipcode,
  p.product_category_id,
  pc.name AS product_category_name,
  b.created_at,
  b.canceled_at,
  b.updated_at,
  b.confirmed_at,
  customer_adress_id,
  b.customer_id,
  language_id,
  start_time

  FROM bookings_view b
  JOIN products_view p ON
  b.product_id = p.id
  JOIN product_categories_view pc ON
  pc.id = p.product_category_id
  JOIN languages_view l ON
  b.language_id = l.id 
  JOIN customer_adresses_view ca ON
  b.customer_adress_id = ca.id
  JOIN countries_view c ON
  ca.country_id = c.id

  WHERE b.created_at >= ?
  AND b.created_at < ?
  ORDER BY b.created_at
`
    return connection.execute(sql, [from, to])
  }
}

// --

function typeCast(field, next) {
  return `[${field.type}] ${field.string()}`
}
