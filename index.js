(function (index) {
    index(window.jQuery, window, document)
}(function ($, window, document) {

    function fetchData() {
        $.getJSON('data/melbGrid.json', function (data) {
            console.log(data);
            renderMap(data)
        });
    }

    function constructPolygon(coordinates) {
        var coords = coordinates.map(function(data, index) {
            return new google.maps.LatLng(data[1], data[0])
        });

        var polygon = new google.maps.Polygon({
            paths: coords,
            draggable: true, // turn off if it gets annoying
            editable: true,
            strokeColor: '#FF0000',
            strokeOpacity: 0.8,
            strokeWeight: 2,
            fillColor: '#FF0000',
            fillOpacity: 0.35
        });
        
        return polygon;
    }

    function renderMap(data) {
        var myLatLng = new google.maps.LatLng(-37.802188, 145.026971);
        // General Options
        var mapOptions = {
            zoom: 9,
            center: myLatLng,
            mapTypeId: google.maps.MapTypeId.RoadMap
        };
        var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

        data.features.map(function(val, index) {
            var polygon = constructPolygon(val.geometry.coordinates[0]);
            polygon.setMap(map);
        });
    }

    $(function () {
        fetchData();
    })
}));

