// Copyright (c) 2026, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/http2_connection.dart';
import 'package:test/test.dart';

import 'src/generated/echo.pbgrpc.dart';

/// Only used as the target of `CONNECT`, never connected to.
const _targetHost = 'example.com';
const _targetPort = 443;

void main() {
  group('CONNECT tunnel', () {
    late _FakeProxy proxy;

    tearDown(() => proxy.close());

    test('targets the server, not the proxy', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(socket, 'HTTP/1.1 200 OK'),
      );

      await expectLater(_connector(proxy).connect(), throwsException);

      expect(
        proxy.requests.single,
        startsWith('CONNECT $_targetHost:$_targetPort HTTP/1.1\r\n'),
      );
      expect(
        proxy.requests.single,
        contains('Host: $_targetHost:$_targetPort'),
      );
    });

    test('brackets IPv6 targets', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(socket, 'HTTP/1.1 200 OK'),
      );

      await expectLater(
        _connector(proxy, host: InternetAddress('::1'), port: 8443).connect(),
        throwsException,
      );

      expect(
        proxy.requests.single,
        startsWith('CONNECT [::1]:8443 HTTP/1.1\r\n'),
      );
    });

    test('proceeds to TLS after a 200 response', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) =>
            _respondAndClose(socket, 'HTTP/1.1 200 Connection established'),
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('TLS handshake through the tunnel failed')),
      );
    });

    test('accepts an HTTP/1.0 response', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) =>
            _respondAndClose(socket, 'HTTP/1.0 200 Connection established'),
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('TLS handshake through the tunnel failed')),
      );
    });

    test('accepts a response split into single bytes', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async {
          for (final byte in latin1.encode('HTTP/1.1 200 OK\r\n\r\n')) {
            socket.add([byte]);
            await socket.flush();
          }
          socket.destroy();
        },
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('TLS handshake through the tunnel failed')),
      );
    });

    test(
      'fails without hanging when CONNECT is rejected',
      () async {
        proxy = await _FakeProxy.start(
          respond: (socket) =>
              _respondAndClose(socket, 'HTTP/1.1 403 Forbidden'),
        );

        await expectLater(
          _connector(
            proxy,
            proxyConnectResponseTimeout: const Duration(seconds: 30),
          ).connect(),
          throwsA(
            _proxyExceptionWith(
              'the proxy rejected CONNECT (HTTP/1.1 403 Forbidden)',
            ),
          ),
        );
      },
      timeout: const Timeout(Duration(seconds: 10)),
    );

    test('reports the scheme required by the proxy', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(
          socket,
          'HTTP/1.1 407 Proxy Authentication Required',
          headers: const ['Proxy-Authenticate: NTLM'],
        ),
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('Proxy authentication is required (NTLM)')),
      );
    });

    test('times out when the proxy does not respond', () async {
      proxy = await _FakeProxy.start(respond: (_) async {});

      await expectLater(
        _connector(
          proxy,
          proxyConnectResponseTimeout: const Duration(milliseconds: 300),
        ).connect(),
        throwsA(_proxyExceptionWith('no response to CONNECT within 300 ms')),
      );
    });

    test('times out when the TLS handshake does not complete', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async {
          socket.write('HTTP/1.1 200 Connection established\r\n\r\n');
          await socket.flush();
          // Swallow the ClientHello and keep the connection open.
        },
      );

      await expectLater(
        _connector(
          proxy,
          tlsHandshakeTimeout: const Duration(milliseconds: 300),
        ).connect(),
        throwsA(
          _proxyExceptionWith('TLS handshake did not complete within 300 ms'),
        ),
      );
    });

    test('fails when the proxy closes the connection', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async => socket.destroy(),
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('closed the connection before responding')),
      );
    });

    test('fails on data after the response when using TLS', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async {
          socket.write('HTTP/1.1 200 OK\r\n\r\nEXTRA');
          await socket.flush();
        },
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('received 5 unexpected bytes')),
      );
    });

    test('forwards data after the response when not using TLS', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async {
          socket.write('HTTP/1.1 200 OK\r\n\r\nEXTRA');
          await socket.flush();
          await Future<void>.delayed(const Duration(milliseconds: 50));
          socket.write('MORE');
          await socket.flush();
          socket.destroy();
        },
      );
      final connector = _connector(
        proxy,
        credentials: const ChannelCredentials.insecure(),
      );

      final incoming = await connector.connectImpl(connector.proxy);

      expect(await incoming.transform(latin1.decoder).join(), 'EXTRAMORE');
    });

    test('fails on a response that is not HTTP', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(socket, 'GARBAGE'),
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('the response to CONNECT is not HTTP')),
      );
    });

    test('fails on a response header that does not end', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) async {
          socket.write('HTTP/1.1 200 OK\r\n');
          socket.write('X-Padding: ${'a' * 20000}\r\n');
          await socket.flush();
        },
      );

      await expectLater(
        _connector(proxy).connect(),
        throwsA(_proxyExceptionWith('did not end within 16384 bytes')),
      );
    });

    test('sends Proxy-Authorization for authenticated proxies', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(socket, 'HTTP/1.1 200 OK'),
      );

      await expectLater(
        _connector(proxy, username: 'user', password: 'pass').connect(),
        throwsException,
      );

      final expected = base64Encode(utf8.encode('user:pass'));
      expect(
        proxy.requests.single,
        contains('proxy-authorization: Basic $expected'),
      );
    });

    test('reports the route when the proxy is unreachable', () async {
      proxy = await _FakeProxy.start(
        respond: (socket) => _respondAndClose(socket, 'HTTP/1.1 200 OK'),
      );
      final connector = _connector(proxy);
      await proxy.close();

      await expectLater(
        connector.connect(),
        throwsA(
          _proxyExceptionWith(
            'Connecting to $_targetHost:$_targetPort via proxy '
            '${proxy.host}:${proxy.port}: could not connect to the proxy',
          ),
        ),
      );
    });
  });

  group('ClientChannel through a proxy', () {
    late Server server;
    late _RelayProxy proxy;
    late ClientChannel channel;

    tearDown(() async {
      await channel.shutdown();
      await proxy.close();
      await server.shutdown();
    });

    test('works without TLS', () async {
      server = Server.create(services: [_EchoService()]);
      await server.serve(address: 'localhost', port: 0);
      proxy = await _RelayProxy.start();

      channel = ClientChannel(
        'localhost',
        port: server.port!,
        options: ChannelOptions(
          credentials: const ChannelCredentials.insecure(),
          proxy: Proxy(host: proxy.host, port: proxy.port),
        ),
      );

      final response = await EchoServiceClient(
        channel,
      ).echo(EchoRequest(message: 'ping'));
      expect(response.message, 'echo: ping');
      expect(proxy.targets, ['localhost:${server.port}']);
    });

    test('works with TLS', () async {
      server = Server.create(services: [_EchoService()]);
      await server.serve(
        address: 'localhost',
        port: 0,
        security: ServerTlsCredentials(
          certificate: File('test/data/localhost.crt').readAsBytesSync(),
          privateKey: File('test/data/localhost.key').readAsBytesSync(),
        ),
      );
      proxy = await _RelayProxy.start();

      channel = ClientChannel(
        'localhost',
        port: server.port!,
        options: ChannelOptions(
          credentials: ChannelCredentials.secure(
            certificates: File('test/data/localhost.crt').readAsBytesSync(),
            authority: 'localhost',
          ),
          proxy: Proxy(host: proxy.host, port: proxy.port),
        ),
      );

      final response = await EchoServiceClient(
        channel,
      ).echo(EchoRequest(message: 'ping'));
      expect(response.message, 'echo: ping');
      expect(proxy.targets, ['localhost:${server.port}']);
    });

    test(
      'fails the call when CONNECT is rejected',
      () async {
        server = Server.create(services: [_EchoService()]);
        await server.serve(address: 'localhost', port: 0);
        proxy = await _RelayProxy.start(rejectWith: 'HTTP/1.1 403 Forbidden');

        channel = ClientChannel(
          'localhost',
          port: server.port!,
          options: ChannelOptions(
            credentials: const ChannelCredentials.insecure(),
            proxy: Proxy(host: proxy.host, port: proxy.port),
          ),
        );

        await expectLater(
          EchoServiceClient(channel).echo(EchoRequest(message: 'ping')),
          throwsA(
            isA<GrpcError>()
                .having((e) => e.code, 'code', StatusCode.unavailable)
                .having(
                  (e) => e.message,
                  'message',
                  contains('the proxy rejected CONNECT'),
                ),
          ),
        );
      },
      timeout: const Timeout(Duration(seconds: 10)),
    );
  });
}

SocketTransportConnector _connector(
  _FakeProxy proxy, {
  Object host = _targetHost,
  int port = _targetPort,
  ChannelCredentials credentials = const ChannelCredentials.secure(),
  Duration proxyConnectResponseTimeout = const Duration(seconds: 5),
  Duration tlsHandshakeTimeout = const Duration(seconds: 5),
  String? username,
  String? password,
}) {
  return SocketTransportConnector(
    host,
    port,
    ChannelOptions(
      credentials: credentials,
      connectTimeout: const Duration(seconds: 5),
      proxyConnectResponseTimeout: proxyConnectResponseTimeout,
      tlsHandshakeTimeout: tlsHandshakeTimeout,
      proxy: Proxy(
        host: proxy.host,
        port: proxy.port,
        username: username,
        password: password,
      ),
    ),
  );
}

Matcher _proxyExceptionWith(String fragment) => isA<ProxyConnectionException>()
    .having((e) => e.message, 'message', contains(fragment));

Future<void> _respondAndClose(
  Socket socket,
  String statusLine, {
  List<String> headers = const [],
}) async {
  final response = StringBuffer('$statusLine\r\n');
  for (final header in headers) {
    response.write('$header\r\n');
  }
  response.write('\r\n');
  socket.write(response.toString());
  await socket.flush();
  socket.destroy();
}

/// Reads a `CONNECT` request header from [socket] and calls [onRequest] with
/// it. Data received after the header is passed to [onData].
void _readConnectRequest(
  Socket socket, {
  required void Function(String request) onRequest,
  void Function(List<int> data)? onData,
}) {
  final received = <int>[];
  var headerReceived = false;
  socket.listen(
    (chunk) {
      if (headerReceived) {
        onData?.call(chunk);
        return;
      }
      received.addAll(chunk);
      final request = latin1.decode(received);
      final headerEnd = request.indexOf('\r\n\r\n');
      if (headerEnd < 0) return;
      headerReceived = true;
      onRequest(request.substring(0, headerEnd + 4));
      final rest = received.sublist(headerEnd + 4);
      if (rest.isNotEmpty) onData?.call(rest);
    },
    onError: (Object _) {},
    onDone: () {},
  );
}

/// A proxy that answers `CONNECT` with a canned response.
class _FakeProxy {
  final ServerSocket _server;
  final _accepted = <Socket>[];
  final requests = <String>[];

  _FakeProxy._(this._server);

  String get host => _server.address.address;
  int get port => _server.port;

  static Future<_FakeProxy> start({
    required Future<void> Function(Socket socket) respond,
  }) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = _FakeProxy._(server);
    server.listen((socket) {
      proxy._accepted.add(socket);
      _readConnectRequest(
        socket,
        onRequest: (request) {
          proxy.requests.add(request);
          respond(socket).ignore();
        },
      );
    });
    return proxy;
  }

  Future<void> close() async {
    await _server.close();
    for (final socket in _accepted) {
      socket.destroy();
    }
  }
}

/// A proxy that tunnels `CONNECT` requests to their target.
class _RelayProxy {
  final ServerSocket _server;
  final _sockets = <Socket>[];
  final targets = <String>[];

  _RelayProxy._(this._server);

  String get host => _server.address.address;
  int get port => _server.port;

  static Future<_RelayProxy> start({String? rejectWith}) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = _RelayProxy._(server);
    server.listen((client) {
      proxy._sockets.add(client);
      final pending = <List<int>>[];
      Socket? upstream;
      _readConnectRequest(
        client,
        onRequest: (request) async {
          if (rejectWith != null) {
            await _respondAndClose(client, rejectWith);
            return;
          }
          final target = request.split(' ')[1];
          proxy.targets.add(target);
          final separator = target.lastIndexOf(':');
          final socket = await Socket.connect(
            target.substring(0, separator),
            int.parse(target.substring(separator + 1)),
          );
          proxy._sockets.add(socket);
          upstream = socket;
          client.write('HTTP/1.1 200 Connection established\r\n\r\n');
          socket.listen(
            client.add,
            onError: (Object _) => client.destroy(),
            onDone: client.destroy,
          );
          pending.forEach(socket.add);
          pending.clear();
        },
        onData: (data) {
          final socket = upstream;
          if (socket == null) {
            pending.add(data);
          } else {
            socket.add(data);
          }
        },
      );
    });
    return proxy;
  }

  Future<void> close() async {
    await _server.close();
    for (final socket in _sockets) {
      socket.destroy();
    }
  }
}

class _EchoService extends EchoServiceBase {
  @override
  Future<EchoResponse> echo(ServiceCall call, EchoRequest request) async =>
      EchoResponse(message: 'echo: ${request.message}');

  @override
  Stream<ServerStreamingEchoResponse> serverStreamingEcho(
    ServiceCall call,
    ServerStreamingEchoRequest request,
  ) => throw UnimplementedError();
}
