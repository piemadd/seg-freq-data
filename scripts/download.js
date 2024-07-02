const fs = require('fs');
const fetch = require("node-fetch");

fetch('https://transitfeeds.com/p/chicago-transit-authority/165/1383269401/download')
  .then((res) => {
    if (res.status !== 200) throw new Error('Error downloading zip')

    const dest = fs.createWriteStream(`./feed.zip`)
    res.body.pipe(dest);
    res.body.on("end", () => {
      console.log('Finished downloading gtfs feed');
    });
  }
  )
  .catch(e => {
    console.log(e)
  })