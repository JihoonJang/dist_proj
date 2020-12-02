const express = require('express');
const bodyParser = require('body-parser');
const dns = require('dns');
const querystring = require('querystring');
const http = require('http');

const app = express();
app.use(bodyParser.urlencoded({limit: "50mb", extended: true}));

const port = 8080;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

main();

function main() {
    console.log(process.env.TYPE);
    switch(process.env.TYPE) {
        case "MAPPER":
            mapper();
            break;
        case "REDUCER":
            reducer();
            break;
        case "MASTER":
            master();
            break;
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function mapper() {
    app.post('/mapper', (req, res) => {
        const {str, reducerIps} = req.body;
        console.log("mapper get post");
        console.log(str);
        console.log(reducerIps);
        words = {};
        letters = {};

        word = "";
        for (let c of str) {
            if (c === " ") {
                if (!words.hasOwnProperty(word)) {
                    words[word] = 0;
                }
                words[word]++;
                word = "";
            }
            else {
                if (!letters.hasOwnProperty(c)) {
                    letters[c] = 0;
                }
                word += c;
                letters[c]++;
            }
        }

        for (let reducerIp of reducerIps) {
            let post_data = querystring.stringify({
                words, letters,
            });

            let post_options = {
                host: reducerIp,
                port: '8080',
                path: '/reducer',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(post_data)
                }
            };

            let post_req = http.request(post_options, function(res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                    console.log('Response: ' + chunk);
                });
            });

            // post the data
            post_req.write(post_data);
            post_req.end();
        }
        res.send({
            words: words,
            letters: letters,
        });
    });

    app.listen(port, () => console.log(`listening on port ${port} at mapper`));
}

async function reducer() {
    app.post('/reducer', (req, res) => {
        const {words, letters} = req.body;
        console.log("reducer get post");
        console.log(words);
        console.log(letters);
        for (let key of Object.keys(words)) {
            words[key] = words[key].reduce((p, c) => p + c, 0);
        }
        for (let key of Object.keys(letters)) {
            letters[key] = letters[key].reduce((p, c) => p + c, 0);
        }
        res.send({
            words, letters,
        });
    });

    app.listen(port, () => console.log(`listening on port ${port} at reducer`));
}

async function master() {
    app.post('/master', async (req, res) => {
        const {text, chunk} = req.body;

        const mapperHost = process.env.MAPPER_HOST;
        const reducerHost = process.env.REDUCER_HOST;


        const mapperIps = await new Promise((resolve, reject) => {
            dns.lookup(mapperHost, {all: true}, (err, addresses) => {
                if (err) reject(err);
                resolve(addresses.map(v => v.address));
            });
        });

        const reducerIps = await new Promise((resolve, reject) => {
            dns.lookup(reducerHost, {all: true}, (err, addresses) => {
                if (err) reject(err);
                resolve(addresses.map(v => v.address));
            });
        });

        for (let mapperIp of mapperIps) {
            let post_data = querystring.stringify({
                str: "a bb c d ee g b c dd sgs dd s",
                reducerIps
            });

            let post_options = {
                host: mapperIp,
                port: '8080',
                path: '/mapper',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(post_data)
                }
            };

            let post_req = http.request(post_options, function(res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                    console.log('Response: ' + chunk);
                });
            });

            // post the data
            post_req.write(post_data);
            post_req.end();
        }
        res.send("OK");
    });

    app.listen(port, () => console.log(`listening on port ${port} at master`));


}