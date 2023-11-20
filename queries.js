//jshint esversion: 11
//jshint -W033
const snakeCase = require('just-snake-case')
const pull = require('pull-stream')

module.exports = function(execute) {

  return {
    entries,
    bookings
  }

  function entries(from, to, barcodeable) {
    //const barcodeable = 'BookingSeat' // 'TicketSale' // 'Booking'

    const conditions = [
      'bse.is_entry IS TRUE',
      'bse.is_valid IS TRUE',
      'bse.entry_at >= ?',
      'bse.entry_at < ?',

      `bc.barcodeable_type = '${barcodeable}'`,
    ]
    const additional_joins = []
    const additional_fields = []
    if (barcodeable == 'Booking') {
      additional_joins.push(`
        LEFT JOIN bookings_view bo
          ON bc.barcodeable_id = bo.id
      `)
    } else {
      const table = snakeCase(barcodeable) + 's_view'
      additional_joins.push(`
        LEFT JOIN ${table} barcodeable
          ON bc.barcodeable_id = barcodeable.id
        LEFT JOIN bookings_view bo
          ON barcodeable.booking_id = bo.id
      `)
    }
    if (barcodeable == 'TicketSale') {
      additional_fields.push('barcodeable.start_at AS ticket_start_at')
      //additional_fields.push('barcodeable.entry_duration AS ticket_entry_duration')
      additional_fields.push('barcodeable.end_at AS ticket_end_at')
    }
    const sql = `
SELECT 
  bse.id AS scan_id,
  barcode_id,
  entry_at,
  people_count,
  position,
  bc.barcodeable_type AS foreign_type,
  bc.barcodeable_id AS foreign_id,
 
  bo.id AS booking_id,
  bo.start_time AS booking_start_time,
  bo.language_id AS booking_language_id,
  
  p.id AS product_id,
  p.name AS product_name,
  p.product_category_id,

  ca.zip AS address_zip,
  ca.country_id AS address_country_id

  ${(additional_fields.length > 0 ? ',' : '') + additional_fields.join(',')}

FROM barcode_scan_events_view bse
LEFT JOIN barcodes_view bc
  ON bse.barcode_id = bc.id

${additional_joins.join('\n')}

  LEFT JOIN products_view p ON
    bo.product_id = p.id

  LEFT JOIN customer_adresses_view ca ON
    bo.customer_adress_id = ca.id

WHERE ${conditions.join('\nAND ')}
ORDER BY bse.id
  `
    return pull(
      execute(sql, [from, to]),
      extract(['booking', 'product', 'address', 'ticket'])
    )
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
    return execute(sql, [from, to])
  }
}

// util

function extract(prefixes) {
  return pull.through(x=> {
    const subs = {}
    for (const k in x) {
      const v = x[k]
      if (v == null) {
        delete x[k]
        continue
      }
      for (const pf of prefixes) {
        if (k.startsWith(pf + '_')) {
          const newKey = k.slice(pf.length + 1)
          if (x[pf] == undefined) x[pf] = {}
          x[pf][newKey] = v
          delete x[k]
        }
      }
    }
  })
}
