const snakeCase = require('just-snake-case')

module.exports = function(connection) {

  return {
    entries,
    bookings
  }

  function entries(from, to) {
    const barcodeable = 'BookingSeat' // 'TicketSale' // 'Booking'

    const conditions = [
      'bse.is_entry IS TRUE',
      'bse.is_valid IS TRUE',
      'bse.entry_at >= ?',
      'bse.entry_at < ?',

      `bc.barcodeable_type = '${barcodeable}'`,
    ]
    const additional_joins = []
    if (barcodeable == 'Booking') {
      additional_joins.push(`
        INNER JOIN bookings_view bo
          ON bc.barcodeable_id = bo.id
      `)
    } else {
      const table = snakeCase(barcodeable) + 's_view'
      additional_joins.push(`
        INNER JOIN ${table} barcodeable
          ON bc.barcodeable_id = barcodeable.id
        LEFT JOIN bookings_view bo
          ON barcodeable.booking_id = bo.id
      `)

    }
    const sql = `
SELECT 
  entry_at,
  people_count,
  position,
  bc.barcodeable_type AS foreign_type,
  bc.barcodeable_id AS foreign_id,
 
  bo.id AS booking_id,
  bo.start_time

FROM barcode_scan_events_view bse
INNER JOIN barcodes_view bc
  ON bse.barcode_id = bc.id

${additional_joins.join('\n')}

WHERE ${conditions.join('\nAND ')}
ORDER BY bse.entry_at
  `
    return connection.execute(sql, [from, to])
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
