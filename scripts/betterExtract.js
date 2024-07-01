const fs = require('fs');
const unzip = require('unzip-stream');
const csv = require('csv-parser');
const turf = require('@turf/turf');

//like 20240504/YYYYMMDD
const dateToDateNum = (date) => {
  return (date.getFullYear() * 10000) + ((date.getMonth() + 1) * 100) + date.getDate();
}

const onlyDo = ['routes', 'trips', 'stops', 'stop_times', 'shapes', 'calendar', 'calendar_dates'];
const weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];

let routes = {};
let trips = {};
let stops = {};
let parentStops = {};
let shapes = {};
let calendar = {};
let stopPairs = {};
let stopPairShapes = {};

let lineFeatures = [];
let stopFeatures = [];

let start = Date.now();
const nowInt = dateToDateNum(new Date());

if (fs.existsSync('./feed')) fs.rmSync('./feed', { recursive: true });
fs.mkdirSync('./feed');

fs.createReadStream('./feed.zip')
  .pipe(unzip.Parse())
  .on('entry', ((entry) => {
    const type = entry.path.split('.')[0];
    if (!onlyDo.includes(type)) return;
    entry.pipe(fs.createWriteStream(`./feed/${entry.path}`));
  }))
  .on('finish', () => {
    console.log(`Done with unzipping in ${((Date.now() - start) / 1000).toFixed(2)}s`);
    start = Date.now();
    fs.createReadStream('./feed/routes.txt')
      .pipe(csv())
      .on('data', (data) => {
        if (data.route_type != '1') return;

        routes[data.route_id] = {
          name: data.route_long_name,
          type: data.route_type,
          trips: {},
        }
      })
      .on('end', () => {
        console.log(`Done with routes ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
        start = Date.now();
        fs.createReadStream('./feed/calendar.txt')
          .pipe(csv())
          .on('data', (data) => {
            const startDate = parseInt(data.start_date);
            const endDate = parseInt(data.end_date);

            //date range is current
            if (startDate < nowInt && endDate >= nowInt) {
              //listing 
              calendar[data.service_id] = weekdays.filter((weekday) => data[weekday] === '1');
            }
          })
          .on('end', () => {
            console.log(`Done with calendar ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
            start = Date.now();
            fs.createReadStream('./feed/trips.txt')
              .pipe(csv())
              .on('data', (data) => {
                if (!calendar[data.service_id]) return; //trip is not currently valid

                if (!routes[data.route_id]) return; //route doesnt exist

                routes[data.route_id].trips[data.trip_id] = {
                  shapeID: data.shape_id,
                  days: calendar[data.service_id],
                  stopTimes: []
                }

                // for lookups later on
                trips[data.trip_id] = data.route_id;
              })
              .on('end', () => {
                console.log(`Done with trips ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                start = Date.now();
                fs.createReadStream('./feed/stops.txt')
                  .pipe(csv())
                  .on('data', (data) => {
                    if (data.parent_station.length > 0) {
                      parentStops[data.stop_id] = data.parent_station;
                      return;
                    }

                    stops[data.stop_id] = {
                      name: data.stop_name,
                      lat: parseFloat(data.stop_lat),
                      lon: parseFloat(data.stop_lon),
                      active: false,
                    };
                  })
                  .on('end', () => {
                    console.log(`Done with stops ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                    start = Date.now();
                    fs.createReadStream('./feed/stop_times.txt')
                      .pipe(csv())
                      .on('data', (data) => {
                        if (!trips[data.trip_id]) return; //not a valid trip
                        const routeID = trips[data.trip_id];
                        if (!routes[routeID]) return; //not a valid route

                        const stopID = parentStops[data.stop_id] ?? data.stop_id;
                        routes[routeID].trips[data.trip_id].stopTimes.push({
                          stopID: stopID,
                          departureTime: data.departure_time,
                          departureTimeInt: parseInt(data.departure_time.replaceAll(':', '')),
                          stopSequence: parseInt(data.stop_sequence)
                        })

                        stops[stopID].active = true
                      })
                      .on('end', async () => {
                        console.log(`Done with stop times ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                        start = Date.now();

                        fs.createReadStream('./feed/shapes.txt')
                          .pipe(csv())
                          .on('data', (data) => {
                            if (!shapes[data.shape_id]) {
                              shapes[data.shape_id] = [];
                            }

                            //including the sequence number so we can sort it later. most agencies have points pre sorted, but edge cases are super fun
                            shapes[data.shape_id].push([data.shape_pt_lon, data.shape_pt_lat, data.shape_pt_sequence]);
                          })
                          .on('end', async () => {
                            //sorting shapes
                            Object.keys(shapes).forEach((shapeID) => {
                              shapes[shapeID] = shapes[shapeID].sort((a, b) => a[2] - b[2]).map((n) => [Number(n[0]), Number(n[1])]);
                            })

                            console.log(`Done with shapes ingestion in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                            start = Date.now();

                            // finding stop pairs and calculating vehicles per hour
                            const routeKeys = Object.keys(routes);
                            for (let i = 0; i < routeKeys.length; i++) {

                              const route = routes[routeKeys[i]];
                              const tripKeys = Object.keys(route.trips);
                              for (let j = 0; j < tripKeys.length; j++) {

                                const trip = route.trips[tripKeys[j]];
                                const stopTimes = trip.stopTimes
                                  .sort((a, b) => a.stopSequence - b.stopSequence); //sorting stop times
                                for (let k = 0; k < stopTimes.length - 1; k++) {

                                  //getting meta for the pair
                                  const thisStopTime = stopTimes[k];
                                  const nextStopTime = stopTimes[k + 1];

                                  const thiStopTimeStopID = parentStops[thisStopTime.stopID] ?? thisStopTime.stopID;
                                  const nextStopTimeStopID = parentStops[nextStopTime.stopID] ?? nextStopTime.stopID;

                                  const stopPairCode = `${thiStopTimeStopID}_${nextStopTimeStopID}`;
                                  const stopPairHour = thisStopTime.departureTime.split(':')[0];

                                  //need to add all possible days of week and hours
                                  if (!stopPairs[stopPairCode]) {
                                    stopPairs[stopPairCode] = {};
                                    for (let l = 0; l < weekdays.length; l++) { //days of week
                                      const weekday = weekdays[l];
                                      stopPairs[stopPairCode][weekday] = {};
                                      for (let m = 0; m < 24; m++) { //hours of day
                                        stopPairs[stopPairCode][weekday][String(m).padStart(2, '0')] = 0;
                                      }
                                    }
                                  }

                                  //generating actual line segment if it doesnt exist
                                  if (!stopPairShapes[stopPairCode]) {
                                    //getting the shape
                                    const tripShape = turf.lineString(shapes[trip.shapeID]);

                                    //getting the points
                                    const startPoint = turf.point([stops[thisStopTime.stopID].lon, stops[thisStopTime.stopID].lat]);
                                    const endPoint = turf.point([stops[nextStopTime.stopID].lon, stops[nextStopTime.stopID].lat]);

                                    //getting segment
                                    const slicedShape = turf.lineSlice(startPoint, endPoint, tripShape);

                                    stopPairShapes[stopPairCode] = slicedShape;
                                  }

                                  //adding the counts for each day
                                  for (let l = 0; l < trip.days.length; l++) {
                                    if (stopPairHour >= 24) { // roll into the next day
                                      if (l === trip.days.length - 1) { // at the end of the array
                                        stopPairs[stopPairCode][trip.days[0]][stopPairHour - 24]++;
                                        continue;
                                      }
                                      stopPairs[stopPairCode][trip.days[l + 1]][stopPairHour - 24]++; // any other point in the array
                                      continue;
                                    }

                                    stopPairs[stopPairCode][trip.days[l]][stopPairHour]++;
                                  }
                                }
                              }
                            }

                            console.log(`Done with stop pairs in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                            start = Date.now();

                            const stopPairShapesKeys = Object.keys(stopPairShapes);
                            for (let i = 0; i < stopPairShapesKeys.length; i++) {
                              lineFeatures.push({
                                ...stopPairShapes[stopPairShapesKeys[i]],
                                properties: {
                                  segment: stopPairShapesKeys[i],
                                  timings: stopPairs[stopPairShapesKeys[i]]
                                }
                              })
                            }

                            const stopKeys = Object.keys(stops);
                            for (let i = 0; i < stopKeys.length; i++) {
                              const stop = stops[stopKeys[i]];

                              if (!stop.active) return; //dont show the stop

                              stopFeatures.push({
                                type: "Feature",
                                properties: {
                                  name: stop.name,
                                  stopID: stopKeys[i],
                                },
                                geometry: {
                                  coordinates: [
                                    stop.lon,
                                    stop.lat
                                  ],
                                  type: "Point"
                                }
                              })
                            }

                            console.log(`Done with feature gen in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                            start = Date.now();


                            if (fs.existsSync('./out')) fs.rmSync('./out', { recursive: true });
                            fs.mkdirSync('./out');

                            fs.writeFileSync('./out/lines.geojson', JSON.stringify({
                              type: "FeatureCollection",
                              features: lineFeatures
                            }));
                            fs.writeFileSync('./out/stops.geojson', JSON.stringify({
                              type: "FeatureCollection",
                              features: stopFeatures
                            }))
                            fs.writeFileSync('./out/pairs.json', JSON.stringify(stopPairs));
                            fs.writeFileSync('./out/index.html', fs.readFileSync('./index.html'));

                            console.log(`Done with writing to files in ${((Date.now() - start) / 1000).toFixed(2)}s`);
                          })
                      })
                  })
              })
          })
      })
  });