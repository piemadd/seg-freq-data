const fs = require('fs');
const gtfs = require('gtfs-stream');

let routes = {};
let trips = {};
let stops = {};
let stopTimes = {};
let shapes = {};
let calendar = [];
let calendarDates = [];

const start = Date.now();

fs.createReadStream('./feed.zip')
  .pipe(gtfs({ raw: true }))
  .on('data', (entity) => {
    switch (entity.type) {
      case 'route':
        routes[entity.route_id] = {
          name: entity.route_long_name,
          type: entity.route_type,
        }
        break;
      case 'trip':
        trips[entity.trip_id] = {
          route: entity.route_id,
          shape: entity.shape_id,
        }
        break;
      case 'stop':
        stops[entity.stop_id] = {
          name: entity.stop_name,
          lat: Number(entity.stop_lat),
          lon: Number(entity.stop_lon),
        }
        break;
      case 'stop_time':
        if (!stopTimes[entity.trip_id]) stopTimes[entity.trip_id] = [];
        stopTimes[entity.trip_id].push({
          id: entity.stop_id,
          seq: entity.stop_sequence,
          dep: entity.departure_time // might have to ternary this if dep doesn't always exist
        })
        break;
      case 'shape':
        if (!shapes[entity.shape_id]) shapes[entity.shape_id] = [];
        shapes[entity.shape_id].push([entity.shape_pt_lon, entity.shape_pt_lat, entity.shape_pt_sequence])
        break;
      case 'calendar':
        if (entity.monday == '1' && entity.tuesday == '1' && entity.wednesday == '1' && entity.thursday == '1' && entity.friday == '1') {
          calendar.push({
            id: entity.service_id,
            startDate: entity.start_date,
            endDate: entity.end_date
          })
        }
        break;
      case 'calendar_date':
        calendarDates.push(entity.date);
        break;
      default:
        //console.log(entity.type)
        break;
    }
  })
  .on('finish', () => {
    const end = Date.now();
    console.log('finished')
    console.log((end - start) / (1000))
  })