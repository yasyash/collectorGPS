const request = require('request');
const Stations = require('../models/stations');
const Sensors_data = require('../models/fake_data');
const Injection = require('../models/injection');
const Equipments = require('../models/equipments');

const format = require('node.date-time');
const isEmpty = require('lodash.isempty');
const merge = require('lodash.merge');

var aspapi_codes_inv = {
    "Пыль общая": "P001",
    "PM1": "PM1",
    "PM2.5": "PM2.5",
    "PM10": "PM10",
    "NO2": "P005",
    "NO": "P006",
    "NH3": "P019",
    "бензол": "P028",
    "HF": "P030",
    "HCl": "P015",
    "м,п-ксилол": "м,п-ксилол",
    "о-ксилол": "о-ксилол",
    "O3": "P007",
    "H2S": "P008",
    "SO2": "P002",
    "стирол": "P068",
    "толуол": "P071",
    "CO": "P004",
    "фенол": "P010",
    "CH2O": "P022",
    "хлорбензол": "P077",
    "этилбензол": "P083"
};

function injector() {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

    Injection
        .where('is_present', true)
        .fetchAll()
        .then(_stations => {
            station = _stations.toJSON();

            if (station.length > 0) {
                for (key in station) {
                    const between_date = [new Date(station[key].date_time).format('Y-MM-ddTHH:mm:SS'), new Date().format('Y-MM-ddTHH:mm:SS')];

                    console.log('key = ', key);

                    var _stat = fetch_data(station[key].id, station[key].idd, between_date, station[key].last_time, station[key].uri, station[key].code, station[key].token, station[key].indx, station[key].msg_id)
                    // .then(_stat => { console.log('status = ', _stat); })
                    console.log("_stat", _stat);
                }
            }
        }).catch(err => console.log('Database connection failed...', err));


}

async function fetch_data(id, idd, between_date, last_time, uri, code, token, indx, msg_id) {

    var _ms_id = Number(msg_id);
    var _limit = Number(msg_id) + 1;
    var _go_out = false;//flag for exit from while
    var _conn_status = false; //connection result

    var i = 0;
    console.log("msg_id = ", msg_id);
    //begin while
    do {
        console.log("in _limit = ", _limit);

        console.log("i = ", i)
        //if (_go_out) break;

        await Equipments
            .where('idd', idd)
            .where('is_present', 'true')
            .fetchAll()
            .then(_equipments => {

                equipments = _equipments.toJSON();
            }).catch(err => console.log('Database fetching data failed...', err));


        if (equipments.length > 0) {
            var header = {
                "token": token,
                "message": _limit,
                "locality": indx,
                "object": code,
                "date_time": between_date[1] + '+' + '0' + new Date().getTimezoneOffset() / (-60)
            };
            console.log('header = ', header);
            var params = {};
            var marker = {};

            console.log('i = ', i);

            var time_frame = [new Date(new Date(between_date[0]).getTime() + 60000 * i).format('Y-MM-ddTHH:mm:SS'), new Date((new Date(between_date[0]).getTime() + 60000 * (i + 1))).format('Y-MM-ddTHH:mm:SS')];
            console.log('time frame = ', time_frame);

            //if (_go_out) break;

            for (_key in equipments) {
                marker = {};

                await Sensors_data
                    .query('whereBetween', 'date_time', time_frame)
                    .where('serialnum', equipments[_key].serialnum)
                    .orderBy('date_time', 'asc')
                    .fetchAll()
                    .then(_data => {
                        data = _data.toJSON();
                        console.log('data =', data);
                        if (data.length > 0) {//create a pouch with measurements
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
                            //var marker = { 'tmp': '' };

                            if (aspapi_codes_inv[data[index].typemeasure]) {
                                name = String(aspapi_codes_inv[data[index].typemeasure]);

                            }
                            else {
                                name = String(data[index].typemeasure);

                            }
                            console.log('name = ', name);

                            //   console.log('stack data = ', pouch);

                            if (!isEmpty(pouch)) {
                                marker[name] = pouch;
                                //merge(marker, pouch);
                            }

                            //delete (marker['tmp']);
                            //  console.log('pouch = ', marker);

                            //send request
                            if (!isEmpty(marker))
                                merge(params, marker);



                        }
                    }).catch(err => console.log('Database fetching data failed...', err));


            }
            // console.log('params = ', params);
            //console.log('isEmpty = ', !isEmpty(params));

            if (!isEmpty(params)) {
                merge(header, { "params": params });

                console.log('JSON = ', JSON.stringify(header));
                //var new_date_time = new Date(between_date[1]).format('Y-MM-ddTHH:mm:SS') + '+' + '0' + new Date().getTimezoneOffset() / (-60);
                request
                    .defaults({
                        // strictSSL: 'false', // allow us to use our self-signed cert for testing
                        //rejectUnauthorized: 'false',
                        'content-type': 'application/json'
                    });
                request({
                    url: uri, method: 'POST', json: {
                        "token": token,
                        "message": _limit,
                        "locality": indx,
                        "object": code,
                        "date_time": between_date[1],
                        "params": params
                    }
                }, function (err, res, body) {
                    console.log("err = ", err);

                    if (!isEmpty(err)) {
                        var success = false;
                        _conn_status = false;
                    }
                    else {
                        console.log("response = ", body);
                        var success = res.body.success;
                        _conn_status = true;
                    }

                    if (success) {
                        //  if (msg_id > 0) {
                        //msg_id--;
                        let process_time_frame = time_frame[1];
                        //var view_time = between_date[1]; //transaction time
                        // } //else {
                        //  var view_time = last_time; //time not change

                        //}
                        if (_ms_id > 0) {
                            _ms_id--;

                            // if (detect_data(time_frame[1], between_date[1]) > 0) {
                            //    _limit++;
                            console.log("msg_id  = ", _ms_id);

                            //} else {
                            //    _time = between_date[1];
                            //}
                            //console.log("msg_id > 0 but data exist = ", _limit);
                            let _time = time_frame[0];
                            if (time_frame[1] < between_date[1]) {
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
                            console.log("_ms_id = ", _ms_id);

                            console.log("msg_id = ", msg_id);

                            _ms_id++;


                            console.log("_ms_id = ", _ms_id);
                            if (process_time_frame > between_date[0]) {//if less than 1440 measures but time is more than 24 hours

                                injection_update_all_time(id, process_time_frame, process_time_frame, _ms_id)
                                    .then(result => {

                                        //console.log("_ms_id = ", _ms_id);
                                        //console.log("msg_id = ", _ms_id);
                                        //let process_time_frame = new Date((new Date(between_date[1]).getTime() - 86400000)).format('Y-MM-ddTHH:mm:SS'); //value that 24 hours ago from now
                                        //console.log("process time frame = ", process_time_frame);
                                        //console.log('Message id = ', _ms_id, " isn't inserted - server with API", uri, " not avaible now..")
                                        _go_out = true;
                                        //return 0;
                                    });

                            } else {
                                injection_update_msg(id, _ms_id)
                                    .then(result => {

                                        //console.log("_ms_id = ", _ms_id);
                                        //console.log("msg_id = ", _ms_id);
                                        //let process_time_frame = new Date((new Date(between_date[1]).getTime() - 86400000)).format('Y-MM-ddTHH:mm:SS'); //value that 24 hours ago from now
                                        //console.log("process time frame = ", process_time_frame);
                                        //console.log('Message id = ', _ms_id, " isn't inserted - server with API", uri, " not avaible now..")
                                        _go_out = true;
                                        //return 0;
                                    });
                            }
                        } else {
                            console.log("process time frame = ", process_time_frame);

                            injection_update_time(id, process_time_frame).then(result => {
                                //console.log("_ms_id = ", _ms_id);
                                //console.log("msg_id = ", _ms_id);

                                // console.log('Message id = ', _ms_id, " isn't inserted - server with API", uri, " not avaible now..")
                                _go_out = true;
                            })
                        }
                    }

                });
            } else {
                //if message line is empty but time frame exist
                detect_data(time_frame[1], between_date[1]).then(_out => {
                    if ((_out == 0)) { //if previous connection is ok
                        _go_out = true;
                        _ms_id = 0;

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



        i++;
    }
    while (!_go_out)
    console.log('while is out', _go_out, ', limit is ', _limit);
};

async function detect_data(time_in, time_now) {
    if (time_in < time_now) //detecting to records is exist if msg limit id is emty
    {
        let _period = [time_in, time_now];


        await Sensors_data
            .query('whereBetween', 'date_time', _period)
            .fetchAll()
            .then(__datacur => {
                __data = __datacur.toJSON();

            });

        console.log(" data lentgh = ", __data.length);

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
module.exports = injector;