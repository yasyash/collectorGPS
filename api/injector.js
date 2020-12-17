const request = require('request');

const Stations = require('../models/stations');
const Sensors_data = require('../models/sensors_data');
const Injection = require('../models/injection');
const Equipments = require('../models/equipments');

const format = require('node.date-time');
const isEmpty = require('lodash.isempty');
const merge = require('lodash.merge');



async function injector() {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

    await Injection
        .where('is_present', true)
        .where('id', 2) //for esolated pseudo-thread for each API direction
        .fetchAll()
        .then(_stations => {
            station = _stations.toJSON();

            if (station.length > 0) {
                for (key in station) {
                    const between_date = [new Date(station[key].date_time).format('Y-MM-ddTHH:mm:SS'), new Date().format('Y-MM-ddTHH:mm:SS')];

                    var _stat = fetch_data(station[key].id, station[key].idd, between_date, station[key].last_time, station[key].uri, station[key].code, station[key].token, station[key].indx, station[key].msg_id)
                }
            }
        }).catch(err => console.log('Database connection failed...', err));


}

async function fetch_data(id, idd, between_date, last_time, uri, code, token, indx, msg_id) {

    var _ms_id = Number(msg_id);
    var _limit = Number(msg_id) + 1;
    var _go_out = false;//flag for exit from while
    var _conn_status = false; //connection result
    var planty = [];
    var data = [];
    var i = 0;
    console.log("msg_id = ", msg_id);
    // cursors prepare
    await Equipments
        .where('idd', idd)
        .where('is_present', 'true')
        .fetchAll()
        .then(_equipments => {

            equipments = _equipments.toJSON();
        }).catch(err => console.log('Database fetching data failsed...', err));

    //if not records detection
    await Sensors_data
        .query('whereBetween', 'date_time', between_date)
        .orderBy('date_time', 'asc')
        .fetchAll()
        .then(_planty => {
            planty = _planty.toJSON();
            if (planty.length == 0)
                _go_out = true;

        });
    console.log("BETWEEN", between_date);
    console.log("total records is ", planty.length);

    //begin while
    while (!_go_out) {
        console.log("iteration = ", i)
        //if (_go_out) break;



        if (equipments.length > 0) {

            var params = {};
            var marker = {};


            var time_frame = [new Date(new Date(between_date[0]).getTime() + 60000 * i).format('Y-MM-ddTHH:mm:SS'), new Date((new Date(between_date[0]).getTime() + 60000 * (i + 1))).format('Y-MM-ddTHH:mm:SS')];
            if (new Date(between_date[1]).getTime() < new Date((new Date(between_date[0]).getTime() + 60000 * (i + 1))))
                _go_out = true;
            //if (_go_out) break;
            console.log('data = ', planty);

            for (_key in equipments) {
                marker = {};
                //console.log("mKEYS = ", equipments[_key]);

                await Sensors_data
                    .query('whereBetween', 'date_time', time_frame)
                    .where('serialnum', equipments[_key].serialnum)
                    .orderBy('date_time', 'asc')
                    .fetchAll()
                    .then(_data => {
                        data = _data.toJSON();


                        if (data.length > 0) {//create a pouch with measurements
                            // console.log("data ----", data)
                            var pouch = {};

                            for (index in data) {

                                pouch = ({
                                    date_time: new Date(data[index].date_time).format('Y-MM-ddTHH:mm:SS') + '+' + '0' + new Date().getTimezoneOffset() / (-60),
                                    serialnum: equipments[_key].serialnum,
                                    unit: equipments[_key].unit_name,
                                    measure: data[index].measure
                                });


                            }
                            var name = "";

                            if (aspapi_codes_inv[data[index].typemeasure]) {
                                name = String(aspapi_codes_inv[data[index].typemeasure]);

                            }
                            else {
                                name = String(data[index].typemeasure);

                            }


                            if (!isEmpty(pouch)) {
                                marker[name] = pouch;
                            }


                            if (!isEmpty(marker))
                                merge(params, marker);



                        }

                    }).catch(err => console.log('Database fetching data failed...', err));


            }

            if (!isEmpty(params)) {

                /* console.log('JSON = ', JSON.stringify({
                     url: uri, method: 'POST', json: {
                         "token": token,
                         "message": _limit,
                         "locality": indx,
                         "object": code,
                         "date_time": between_date[1],
                         "params": params
                     }
                 }));*/

                request
                    .defaults({
                        // strictSSL: 'false', // allow us to use our self-signed cert for testing
                        //rejectUnauthorized: 'false',
                        'content-type': 'application/json'
                    });
                console.log("in request")

                request({
                    url: uri, method: 'POST', json: {
                        "token": token,
                        "message": _limit,
                        "locality": indx,
                        "object": code,
                        "date_time": between_date[1],
                        "params": params
                    }
                },
                    function (err, res, body) {
                        // console.log("out request")
                        if (!isEmpty(err)) {
                            var success = false;
                            _conn_status = false;
                            console.log("err = ", err);
                            _go_out = true;
                        }
                        else {
                            console.log("response = ", res.body);
                            //console.log("response all= ", res);

                            var success = res.body.success;
                            console.log("status = ", success);
                            _conn_status = true;
                        }

                        if (success) {

                            let process_time_frame = time_frame[1];

                            if (_ms_id > 0) {
                                _ms_id--;

                                let _time = time_frame[0];
                                if (new Date(time_frame[1]).getTime() < new Date(between_date[1]).getTime()) {
                                    _time = time_frame[1];
                                }
                                injection_update_all_time(id, _time, between_date[1], _ms_id);

                            } else {
                                //if message line is empty but time frame exist
                                let _time = time_frame[1];
                                detect_data(time_frame[1], between_date[1]).then(_out => {
                                    if (_out > 0) {
                                        _limit++;
                                        console.log("msg_id = 0 but data exist = ", _limit);

                                    } else {
                                        _time = between_date[1];
                                        _go_out = true;
                                    }
                                    if (time_frame[1] > between_date[1]) {

                                        _time = time_frame[0];

                                    }
                                    //if line is out
                                    injection_update_all_time(id, _time, between_date[1], 0);


                                });


                            }


                        }
                        else {
                            let process_time_frame = new Date((new Date(between_date[1]).getTime() - 86400000)).format('Y-MM-ddTHH:mm:SS'); //value that 24 hours ago from now

                            if (_ms_id < 1440) {

                                _ms_id++;

                                //console.log("_ms_id = ", _ms_id);
                                if (process_time_frame > between_date[0]) {//if less than 1440 measures but time is more than 24 hours

                                    injection_update_all_time(id, process_time_frame, process_time_frame, _ms_id)
                                        .then(result => {

                                            _go_out = true;
                                        });

                                } else {
                                    injection_update_msg(id, _ms_id)
                                        .then(result => {

                                            _go_out = true;
                                        });
                                }
                            } else {
                                console.log("process time frame = ", process_time_frame);

                                injection_update_time(id, process_time_frame).then(result => {

                                    _go_out = true;
                                })
                            }
                        }

                    });
                console.log("out ====")
            } else {
                console.log("else ====")

                //if message line is empty but time frame exist
                detect_data(time_frame[1], between_date[1]).then(_out => {
                    if ((_out == 0)) { //if previous connection is ok
                        _go_out = true;
                        _ms_id = 0;
                        // console.log("_out", _conn_status)
                        //if line is out
                        if (_conn_status)
                            injection_update_all_time(id, between_date[1], between_date[1], 0)
                                .then(result => {
                                    console.log('Emty results');
                                });
                    }
                });

            }
            // end rquest

        }

        //   console.log("i = ", i)
        //  if (i > 100) _go_out = true;
        i++;
    } // end while cycle

    console.log('injection is completed...');
};

async function detect_data(time_in, time_now) {
    if (new Date(time_in).getTime() < new Date(time_now).getTime()) //detecting to records is exist if msg limit id is emty
    {
        let _period = [time_in, time_now];


        await Sensors_data
            .query('whereBetween', 'date_time', _period)
            .fetchAll()
            .then(__datacur => {
                __data = __datacur.toJSON();

            });

    } else {
        return 0;
    }

    if (__data.length > 0) {
        return 1;
    } else {
        return 0;
    }
}

async function injection_update_all_time(id, _time, last_time, msg_id) {
    await Injection.where({ id: id })
        .save({
            date_time: _time,
            last_time: last_time,
            msg_id: msg_id
        }, { patch: true })
        .then(result => {
            console.log("Message id =  is inserted at ", last_time, " from ", _time);
        }).catch(err => console.log("Update Injection table error...", err));
}

async function injection_update_msg(id, msg_id) {
    await Injection.where({ id: id })
        .save({

            msg_id: msg_id
        }, { patch: true })
        .then(result => {
            console.log("Message id updated");
        }).catch(err => console.log("Update Injection table error...", err));
}

async function injection_update_time(id, _time) {
    await Injection.where({ id: id })
        .save({
            date_time: _time,

        }, { patch: true })
        .then(result => {
            console.log("Datetime is updated... ");
        }).catch(err => console.log("Update Injection table error...", err));
}

async function coordinate_update(lat, long) {
    await Stations.where({ is_present: true })
        .save({
            'latitude': lat,
            'longitude': long

        }, {
          patch: true
        })
        .then(result => {
            console.log("Coordinates is updated... ");
        }).catch(err => console.log("Update coordinates error...", err));
}

function try_gps() {

    var options = {
        headers: {
            'Content-Type': 'application/json'
        },
        uri: 'https://map.gpshome.ru/api/get_position.php',
        method: 'GET',
        qs: {
            'userid': 130188,
            'key': "0197b7a6e2284de59e1a23fe8eb15fe5",
            'objids': 183401

        }
    };
    request(options,
        function (err, res, body) {
            console.log("response = ", body);
            if (err) {
                console.log("err = ", err);


            } else {
                __data = JSON.parse(body);
                if (__data){
                    console.log("response = ", __data.objects[0].lat);
                    coordinate_update(__data.objects[0].lat, __data.objects[0].lon)
                }
            }

        })
}

module.exports = try_gps;
