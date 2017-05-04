'use strict';

var avro = require('avsc'), // nodejs avro implementation
    streams = require('memory-streams'); // used to pipe the avro buffer to IoT Hub message object

// load settings from .env file.
require('dotenv').config();

// Load avro schema from external file, There's an issue with Enums and ASA - that is currently being fixed. For now, you must specify it as a string.
const type = avro.parse(__dirname + '/schema.avsc');

// IoT Hub setup
var Protocol = require('azure-iot-device-amqp').Amqp;
var Client = require('azure-iot-device').Client;
var Message = require('azure-iot-device').Message;
var client = Client.fromConnectionString(process.env.iotHubConnectionString, Protocol);

// IoT Hub Connection Callback
var connectCallback = function (err) {
    if (err) {
        console.error('Could not connect to IoT Hub: ' + err.message);
    } else {
        console.log('Connected to IoT Hub');

        // Send events to IoT Hub on a timer.
        var sendInterval = setInterval(function () {

            // Instantiate a BlockEncoder, which allows you to write avro into a buffer.
            var avroEncoder = new avro.streams.BlockEncoder(type, { codec: 'deflate' }); // Choose 'deflate' or it will default to 'null'

            // Instantiate a stream to write the avro buffer to, which we'll send to IoT Hub
            var writer = new streams.WritableStream();
            avroEncoder.pipe(writer);

            // Generate the faux json
            var windSpeed = 10 + (Math.random() * 4); // range: [10, 14]
            var json = { deviceId: 'device1', windSpeed: windSpeed };

            // Write the json
            if (type.isValid(json)) {
                avroEncoder.write(json);
            }

            // Call end to tell avro we are done writing and to trigger the end event.
            avroEncoder.end();

            // end event was triggered, get the avro data from the piped stream and send to IoT Hub.
            avroEncoder.on('end', function () {
                // call toBuffer on the WriteableStream and pass to IoT Hub message ctor
                var message = new Message(writer.toBuffer());

                console.log('Sending message: ' + message.getData());
                client.sendEvent(message, printResultFor('send'));
            })
        }, 2000);

        client.on('error', function (err) {
            console.error(err.message);
        });

        client.on('disconnect', function () {
            clearInterval(sendInterval);
            client.removeAllListeners();
        });
    }
};

// Create a connection to IoT Hub and then connectCallback.
client.open(connectCallback);

// Helper function to print results in the console
function printResultFor(op) {
    return function printResult(err, res) {
        if (err) console.log(op + ' error: ' + err.toString());
        if (res) console.log(op + ' status: ' + res.constructor.name);
    };
}