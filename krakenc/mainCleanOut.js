const fs = require('fs');

fs.unlink("./out/xlmeur.csv", function (err) {
  if (err) throw err;
  console.log('successfully deleted out/xlmeur.csv');
});
