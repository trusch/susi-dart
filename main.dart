import 'dart:io';
import 'dart:convert';
import 'dart:async';
import 'package:args/args.dart';
import 'package:uuid/uuid.dart';

class Susi {
    String _addr;
    num _port;
    String _key;
    String _cert;
    SecurityContext _context;
    ServerSocket _socket;
    Uuid _uuid = new Uuid();

    Map _consumers = {};
    Map _processors = {};
    Map _publishCompleters = {};

    Map _processorEventProcesses = {};

    JsonEncoder _encoder = new JsonEncoder();
    JsonDecoder _decoder = new JsonDecoder();

    Susi(this._addr, this._port, this._key, this._cert);

    Future connect() async {
        _context = new SecurityContext()
            ..useCertificateChain(_cert)
            ..usePrivateKey(_key);
        await SecureSocket.connect(_addr, 
            _port, 
            onBadCertificate: (X509Certificate c)=>true, 
            context: _context)
        .then((socket) {
            this._socket = socket;
            print('connected');
            socket.transform(UTF8.decoder)
            .transform(new LineSplitter()).listen((String d){
                var doc = _decoder.convert(d);
                if(doc['type'] == 'consumerEvent'){
                    _consumers.forEach((pattern,consumers){
                        var p = new RegExp(pattern);
                        if(p.hasMatch(doc['data']['topic'])){
                            consumers.forEach((_,cb){
                                cb(doc['data']);
                            });
                        }
                    });
                }else if(doc['type'] == 'ack'){
                    var id = doc['data']['id'];
                    if(_publishCompleters.containsKey(id)){
                        _publishCompleters[id].complete(doc['data']);
                        _publishCompleters.remove(id);
                    }
                }else if(doc['type'] == 'dismiss'){
                    var id = doc['data']['id'];
                    if(_publishCompleters.containsKey(id)){
                        _publishCompleters[id].completeError(doc['data']);
                        _publishCompleters.remove(id);
                    }
                }else if(doc['type'] == 'processorEvent'){
                    var event = doc['data'];
                    var process = new List();
                    _processors.forEach((pattern,processors){
                        var regex = new RegExp(pattern);
                        if(regex.hasMatch(event['topic'])){
                            processors.forEach((k,v)=>process.add(v));
                        }
                    });
                    _processorEventProcesses[event['id']] = process;
                    this.ack(event);
                }
            });
        });
    }

    void ack(event){
        var process = _processorEventProcesses[event['id']];
        if(process.length == 0){
            _socket.write(_encoder.convert({
                'type': 'ack',
                'data': event
            }));
            _processorEventProcesses.remove(event['id']);
        }else{
            var next = process[0];
            process.removeAt(0);
            next(event);
        }
    }

    void dismiss(event){
        _socket.write(_encoder.convert({
            'type': 'dismiss',
            'data': event
        }));
        _processorEventProcesses.remove(event['id']);
    }

    registerConsumer(topic,consumer){
        var consumers = null;
        if(_consumers.containsKey(topic)){
            consumers = _consumers[topic];
        }else{
            consumers = {};
        }
        var id = _uuid.v4();
        consumers[id] = consumer;
        if(consumers.length == 1) {
            _socket.write(_encoder.convert({
                'type': 'registerConsumer',
                'data': {
                    'topic': topic
                }
            }));
        }
        _consumers[topic] = consumers;
        return id;
    }

    unregisterConsumer(id){
        _consumers.forEach((pattern,consumers){
            if(consumers.containsKey(id)){
                consumers.remove(id);
            }
            if(consumers.length == 0){
                _socket.write(_encoder.convert({
                    'type': 'unregisterConsumer',
                    'data': {
                        'topic': pattern
                    }
                }));
            }
        });
    }

    registerProcessor(topic,processor){
        var processors = null;
        if(_processors.containsKey(topic)){
            processors = _processors[topic];
        }else{
            processors = {};
        }
        var id = _uuid.v4();
        processors[id] = processor;
        if(processors.length == 1) {
            _socket.write(_encoder.convert({
                'type': 'registerProcessor',
                'data': {
                    'topic': topic
                }
            }));
        }
        _processors[topic] = processors;
        return id;
    }

    unregisterProcessor(id){
        _processors.forEach((pattern,processors){
            if(processors.containsKey(id)){
                processors.remove(id);
            }
            if(processors.length == 0){
                _socket.write(_encoder.convert({
                    'type': 'unregisterProcessor',
                    'data': {
                        'topic': pattern
                    }
                }));
            }
        });
    }

    publish(data){
        if(!data.containsKey('id')){
            data['id'] = _uuid.v4();
        }
        _publishCompleters[data['id']] = new Completer();
        _socket.write(_encoder.convert({
            'type':'publish',
            'data': data
        }));
        return _publishCompleters[data['id']].future;
    }
}

main(List<String> arguments) {
    
    final parser = new ArgParser()
      ..addOption('host', defaultsTo: 'localhost', abbr: 'h')
      ..addOption('port', defaultsTo: 4000, abbr: 'p')
      ..addOption('key',  defaultsTo: 'key.pem', abbr: 'k')
      ..addOption('cert', defaultsTo: 'cert.pem', abbr: 'c');

    ArgResults argResults = parser.parse(arguments);

    print(argResults['host']);
    print(argResults['port']);
    print(argResults['key']);
    print(argResults['cert']);

    var susi = new Susi(argResults['host'],argResults['port'],argResults['key'],argResults['cert']);
    susi.connect().then((_){
        susi.registerProcessor('.*', (event) {
            event['payload'] = {'p1':true};
            print('p1: ${event["payload"]}');
            susi.ack(event);
        });
        susi.registerProcessor('foobar', (event) {
            event['payload']['p2'] = true;
            print('p2: ${event["payload"]}');
            susi.dismiss(event);
        });
        susi.registerProcessor('foobar', (event) {
            event['payload']['p3'] = true;
            print('p3: ${event["payload"]}');
            susi.ack(event);
        });
        susi.registerConsumer('foobar', (event) {
            print('consumer: ${event["payload"]}');
        });
        susi.publish({'topic': 'foobar'})
            .then((event)=>print('got ack: ${event["payload"]}'))
            .catchError((event)=>print('got dismiss: ${event["payload"]}'));
            .whenComplete((event)=>print('finally got this (ack or dismiss): ${event["payload"]}'));
    });
}