const express = require('express');
const bodyParser = require('body-parser');
const dns = require('dns');
const querystring = require('querystring');
const http = require('http');
const fs = require('fs');

const app = express();
app.use(bodyParser.urlencoded({limit: "50mb", extended: true}));

const port = 8080;

String.prototype.hashCode = function() {
    var hash = 0;
    if (this.length == 0) {
        return hash;
    }
    for (var i = 0; i < this.length; i++) {
        var char = this.charCodeAt(i);
        hash = ((hash<<5)-hash)+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}

String.prototype.isalpha = function(i) {
    return (this.charCodeAt(i) >= 'a'.charCodeAt(0) && this.charCodeAt(i) <= 'z'.charCodeAt(0)) || (this.charCodeAt(i) >= 'A'.charCodeAt(0) && this.charCodeAt(i) <= 'Z'.charCodeAt(0));
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

function mapper() {
    app.post('/mapper', function(req, res) {
        const {str, reducerIps, mapper_length, masterIp} = req.body;
        console.log("mapper get post");
        let words = [];
        let letters = [];
        for (let i = 0; i < reducerIps.length; i++) {
            words.push({});
            letters.push({});
        }

        word = "";
        for (let c of str) {
            c = c.toLowerCase();
            if (!c.isalpha(0)) {
                if (word === "") continue;
                let idx = Math.abs(word.hashCode()) % reducerIps.length;

                if (!words[idx].hasOwnProperty(word)) {
                    words[idx][word] = 0;
                }
                words[idx][word]++;
                word = "";
            }
            else {
                let idx = Math.abs(c.hashCode()) % reducerIps.length;
                if (!letters[idx].hasOwnProperty(c)) {
                    letters[idx][c] = 0;
                }
                word += c;
                letters[idx][c]++;
            }
        }
        console.log(words);
        console.log(letters);

        for (let i = 0; i < reducerIps.length; i++) {
            let reducerIp = reducerIps[i];
            let post_data = querystring.stringify({
                words: JSON.stringify(words[i]), 
                letters: JSON.stringify(letters[i]),
                mapper_length,
                masterIp,
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
        res.send("mapper get OK");
    });

    app.listen(port, () => console.log(`listening on port ${port} at mapper`));
}

async function reducer() {
    let total_words;
    let total_letters;
    let mapperCnt;
    app.post('/reducer', function(req, res) {
        const init = req.body.init;

        if (init) {
            total_words = {};
            total_letters = {};
            mapperCnt = 0;
            res.send("init OK");
            return;
        }

        const words = JSON.parse(req.body.words);
        const letters = JSON.parse(req.body.letters);
        const mapper_length = parseInt(req.body.mapper_length);
        const masterIp = req.body.masterIp;

        for (let word of Object.keys(words)) {
            if (!total_words.hasOwnProperty(word)) {
                total_words[word] = 0;
            }
            total_words[word] += words[word];
        }
        for (let letter of Object.keys(letters)) {
            if (!total_letters.hasOwnProperty(letter)) {
                total_letters[letter] = 0;
            }
            total_letters[letter] += letters[letter];
        }

        mapperCnt++;
        if (mapperCnt === mapper_length) {
            let post_data = querystring.stringify({
                words: JSON.stringify(total_words), 
                letters: JSON.stringify(total_letters),
                mapper_length,
            });

            let post_options = {
                host: masterIp,
                port: '8080',
                path: '/master_result',
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

        res.send("reducer get OK");
    });

    app.listen(port, () => console.log(`listening on port ${port} at reducer`));
}

async function init(reducerIps) {
    for (let ip of reducerIps) {
        let post_data = querystring.stringify({
            init: true
        });

        let post_options = {
            host: ip,
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
}

async function master() {
    let reducerCnt;
    let words_res;
    let letters_res;
    let reducer_length;
    let startTime;
    let endTime;
    app.post('/master', async function(req, res) {
        startTime = new Date().getTime();
        const mapperHost = process.env.MAPPER_HOST;
        const reducerHost = process.env.REDUCER_HOST;

        const reducerIps = await new Promise((resolve, reject) => {
            dns.lookup(reducerHost, {all: true}, (err, addresses) => {
                if (err) reject(err);
                resolve(addresses.map(v => v.address));
            });
        });

        init(reducerIps);
        reducerCnt = 0;
        words_res = {};
        letters_res = {};
        reducer_length = reducerIps.length;

        const mapperIps = await new Promise((resolve, reject) => {
            dns.lookup(mapperHost, {all: true}, (err, addresses) => {
                if (err) reject(err);
                resolve(addresses.map(v => v.address));
            });
        });

        const masterIp = await new Promise((resolve, reject) => {
            dns.lookup("master.default.svc.cluster.local", (err, address) => {
                if (err) reject(err);
                resolve(address);
            });
        });

        const chunk_size = req.body.chunk ? parseInt(req.body.chunk) : 512 * 1024;
        const text = fs.readFileSync(req.body.filename, 'utf8').substr(0, chunk_size);
        let mapper_chunk_size = parseInt(text.length / 3);
        console.log("text length: " + text.length);
        console.log("chunk size: " + mapper_chunk_size);

        let start_idx = 0;

        for (let i = 0; i < mapperIps.length; i++) {
            let mapperIp = mapperIps[i];
            if (i === mapperIps.length - 1) mapper_chunk_size = text.length;
            let post_data = querystring.stringify({
                str: text.substr(start_idx, mapper_chunk_size),
                reducerIps,
                mapper_length: mapperIps.length,
                masterIp,
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
            start_idx = Math.min(text.length - 1, start_idx + mapper_chunk_size);
        }
        res.send("chunk sended OK");
    });
    
    app.post("/master_result", function(req, res) {
        const words = JSON.parse(req.body.words);
        const letters = JSON.parse(req.body.letters);

        words_res = Object.assign(words_res, words);
        letters_res = Object.assign(letters_res, letters);

        reducerCnt++;
        if (reducerCnt === reducer_length) {
            endTime = new Date().getTime();

            let sorted_words = Object.entries(words_res).sort((a, b) => b[1] - a[1]);
            let sorted_letters = Object.entries(letters_res).sort((a, b) => b[1] - a[1]);
            fs.writeFileSync("output", `Execute time: ${endTime - startTime}ms\n`);

            fs.appendFileSync("output", `top 5% words\n\n`);
            for (let i = 0; i <= 0.05 * sorted_words.length; i++) {
                let [word, freq] = sorted_words[i];
                fs.appendFileSync("output", `word: ${word}, frequency: ${freq}\n`);
            }

            fs.appendFileSync("output", `47.5% - 52.5% words\n\n`);
            for (let i = parseInt(0.475 * sorted_words.length + 1); i <= 0.525 * sorted_words.length; i++) {
                let [word, freq] = sorted_words[i];
                fs.appendFileSync("output", `word: ${word}, frequency: ${freq}\n`);
            }

            fs.appendFileSync("output", `bottom 5% words\n\n`);
            for (let i = parseInt(0.95 * sorted_words.length + 1); i < sorted_words.length; i++) {
                let [word, freq] = sorted_words[i];
                fs.appendFileSync("output", `word: ${word}, frequency: ${freq}\n`);
            }

            fs.appendFileSync("output", `top 5% letters\n\n`);
            for (let i = 0; i <= 0.05 * sorted_letters.length; i++) {
                let [letter, freq] = sorted_letters[i];
                fs.appendFileSync("output", `letter: ${letter}, frequency: ${freq}\n`);
            }

            fs.appendFileSync("output", `47.5% - 52.5% letters\n\n`);
            for (let i = parseInt(0.475 * sorted_letters.length + 1); i <= 0.525 * sorted_letters.length; i++) {
                let [letter, freq] = sorted_letters[i];
                fs.appendFileSync("output", `letter: ${letter}, frequency: ${freq}\n`);
            }

            fs.appendFileSync("output", `bottom 5% letters\n\n`);
            for (let i = parseInt(0.95 * sorted_letters.length + 1); i < sorted_letters.length; i++) {
                let [letter, freq] = sorted_letters[i];
                fs.appendFileSync("output", `letter: ${letter}, frequency: ${freq}\n`);
            }
        }
        res.send("result get OK");
    });

    app.listen(port, () => console.log(`listening on port ${port} at master`));
}