//jshint esversion: 11
//jshint -W033
const pull = require('pull-stream')

const enum_defs = {
  'zones': {
    table: 'relay_entry_zones_view',
    fields: ['id', 'name']
  },
  'language': {
    table: 'languages_view',
    fields: ['id', 'locale_code', 'name', 'name_intern'],
  },
  'country': {
    table: 'countries_view',
    fields: ['id', 'code', 'name'],
  },
  'product_category_type': {
    table: 'product_category_types_view',
    fields: ['id', 'booking_method', 'name', 'name_intern'],
  }
}

module.exports = function(execute) {
  return {
    get
  }

  function get(cb) {
    pull(
      pull.keys(enum_defs),
      pull.asyncMap( (type, cb)=>{
        const {table, fields} = enum_defs[type]
        pull(
          execute(`SELECT ${fields.join(', ')} FROM ${table}`),
          pull.through(o=>{
            o.type = type;
          }),
          pull.reduce( (acc, x)=>{
            acc[1][x[fields[0]]] = x
            return acc
          }, [type, {}], cb)
        )
      }),
      pull.collect((err, entries)=>{
        if (err) return cb(err)
        cb(null, Object.fromEntries(entries))
      })
    )
  }
}


