var ItemFactory = (function () {

  function generateRandomItems (count, lastIndex) {
    return axios.post('/get_items', {
      'time': [ '2017-01-01 00:00:00', '2018-07-04 00:00:00'],
      'location': { 'name': "American", 'longitude': [ 0.0, 180.0], 'latitude': [ 0.0, 60.0 ] },
      'hashtags': [ 'winterwedding', 'summerwedding' ],
      'last_index': lastIndex,
      'batch': count 
    })
  }

  function getRandomColor () {
    var colors = [
      'rgba(21,174,103,.5)',
      'rgba(245,163,59,.5)',
      'rgba(255,230,135,.5)',
      'rgba(194,217,78,.5)',
      'rgba(195,123,177,.5)',
      'rgba(125,205,244,.5)'
    ]
    return colors[~~(Math.random() * colors.length)]
  }

  return {
    get: generateRandomItems,
    getRandomColor: getRandomColor 
  }

})()

/*

var ItemFactory = (function () {

  var lastIndex = 0

  function generateRandomItems (count) {
    var items = [], i
    for (i = 0; i < count; i++) {
      items[i] = {
        index: lastIndex++,
        style: {
          background: getRandomColor()
        },
        width: 100 + ~~(Math.random() * 50),
        height: 100 + ~~(Math.random() * 50)
      }
    }
    console.log(items);
    console.log(typeof(items));
    console.log(items.length);
    return items
  }

  function getRandomColor () {
    var colors = [
      'rgba(21,174,103,.5)',
      'rgba(245,163,59,.5)',
      'rgba(255,230,135,.5)',
      'rgba(194,217,78,.5)',
      'rgba(195,123,177,.5)',
      'rgba(125,205,244,.5)'
    ]
    return colors[~~(Math.random() * colors.length)]
  }

  return {
    get: generateRandomItems
  }

})()
*/
