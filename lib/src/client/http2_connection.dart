// Copyright (c) 2018, the gRPC project authors. Please see the AUTHORS file
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

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http2/transport.dart';

import '../shared/codec.dart';
import '../shared/timeout.dart';
import 'call.dart';
import 'client_keepalive.dart';
import 'client_transport_connector.dart';
import 'connection.dart' hide ClientConnection;
import 'connection.dart' as connection;
import 'options.dart';
import 'proxy.dart';
import 'transport/http2_credentials.dart';
import 'transport/http2_transport.dart';
import 'transport/transport.dart';

class Http2ClientConnection implements connection.ClientConnection {
  static final _methodPost = Header.ascii(':method', 'POST');
  static final _schemeHttp = Header.ascii(':scheme', 'http');
  static final _schemeHttps = Header.ascii(':scheme', 'https');
  static final _contentTypeGrpc = Header.ascii(
    'content-type',
    'application/grpc',
  );
  static final _teTrailers = Header.ascii('te', 'trailers');

  final ChannelOptions options;

  connection.ConnectionState _state = ConnectionState.idle;

  void Function(connection.ConnectionState)? onStateChanged;

  final _pendingCalls = <ClientCall>[];

  final ClientTransportConnector _transportConnector;
  ClientTransportConnection? _transportConnection;

  /// Used for idle and reconnect timeout, depending on [_state].
  Timer? _timer;

  /// Used for making sure a single connection is not kept alive too long.
  final Stopwatch _connectionLifeTimer = Stopwatch();

  Duration? _currentReconnectDelay;

  ClientKeepAlive? keepAliveManager;

  Http2ClientConnection(Object host, int port, this.options)
    : _transportConnector = SocketTransportConnector(host, port, options);

  Http2ClientConnection.fromClientTransportConnector(
    this._transportConnector,
    this.options,
  );

  ChannelCredentials get credentials => options.credentials;

  @override
  String get authority => _transportConnector.authority;

  @override
  String get scheme => options.credentials.isSecure ? 'https' : 'http';

  ConnectionState get state => _state;

  static const _estimatedRoundTripTime = Duration(milliseconds: 20);

  Future<ClientTransportConnection> connectTransport() async {
    final connection = await _transportConnector.connect();
    _transportConnector.done.then((_) => _abandonConnection());

    // Give the settings settings-frame a bit of time to arrive.
    // TODO(sigurdm): This is a hack. The http2 package should expose a way of
    // waiting for the settings frame to arrive.
    await Future.delayed(_estimatedRoundTripTime);

    if (_state == ConnectionState.shutdown) {
      _transportConnector.shutdown();
      throw _ShutdownException();
    }
    return connection;
  }

  void _connect() {
    if (_state != ConnectionState.idle &&
        _state != ConnectionState.transientFailure) {
      return;
    }
    _setState(ConnectionState.connecting);
    connectTransport()
        .then<void>((transport) async {
          _currentReconnectDelay = null;
          _transportConnection = transport;
          if (options.keepAlive.shouldSendPings) {
            keepAliveManager = ClientKeepAlive(
              options: options.keepAlive,
              ping: () {
                if (transport.isOpen) {
                  transport.ping();
                }
              },
              onPingTimeout: () => transport.finish(),
            );
            transport.onFrameReceived.listen(
              (_) => keepAliveManager?.onFrameReceived(),
            );
          }
          _connectionLifeTimer
            ..reset()
            ..start();
          transport.onActiveStateChanged = _handleActiveStateChanged;
          _setState(ConnectionState.ready);

          if (_hasPendingCalls()) {
            // Take all pending calls out, and reschedule.
            final pendingCalls = _pendingCalls.toList();
            _pendingCalls.clear();
            pendingCalls.forEach(dispatchCall);
          }
        })
        .catchError(_handleConnectionFailure);
  }

  /// Abandons the current connection if it is unhealthy or has been open for
  /// too long.
  ///
  /// Assumes [_transportConnection] is not `null`.
  void _refreshConnectionIfUnhealthy() {
    final isHealthy = _transportConnection!.isOpen;
    final shouldRefresh =
        _connectionLifeTimer.elapsed > options.connectionTimeout;
    if (shouldRefresh) {
      _transportConnection!.finish();
      keepAliveManager?.onTransportTermination();
    }
    if (!isHealthy || shouldRefresh) {
      _abandonConnection();
    }
  }

  @override
  void dispatchCall(ClientCall call) {
    if (_transportConnection != null) {
      _refreshConnectionIfUnhealthy();
    }
    switch (_state) {
      case ConnectionState.ready:
        _startCall(call);
        break;
      case ConnectionState.shutdown:
        _shutdownCall(call);
        break;
      default:
        _pendingCalls.add(call);
        if (_state == ConnectionState.idle) {
          _connect();
        }
    }
  }

  @override
  GrpcTransportStream makeRequest(
    String path,
    Duration? timeout,
    Map<String, String> metadata,
    ErrorHandler onRequestFailure, {
    CallOptions? callOptions,
  }) {
    final compressionCodec = callOptions?.compression;
    final headers = createCallHeaders(
      credentials.isSecure,
      _transportConnector.authority,
      path,
      timeout,
      metadata,
      compressionCodec,
      userAgent: options.userAgent,
      grpcAcceptEncodings:
          (callOptions?.metadata ?? const {})['grpc-accept-encoding'] ??
          options.codecRegistry?.supportedEncodings,
    );
    final stream = _transportConnection!.makeRequest(headers);
    return Http2TransportStream(
      stream,
      onRequestFailure,
      options.codecRegistry,
      compressionCodec,
    );
  }

  void _startCall(ClientCall call) {
    if (call.isCancelled) return;
    call.onConnectionReady(this);
  }

  void _failCall(ClientCall call, dynamic error) {
    if (call.isCancelled) return;
    call.onConnectionError(error);
  }

  void _shutdownCall(ClientCall call) {
    _failCall(call, 'Connection shutting down.');
  }

  @override
  Future<void> shutdown() async {
    if (_state == ConnectionState.shutdown) return;
    _setShutdownState();
    await _transportConnection?.finish();
    keepAliveManager?.onTransportTermination();
  }

  @override
  Future<void> terminate() async {
    _setShutdownState();
    await _transportConnection?.terminate();
    keepAliveManager?.onTransportTermination();
  }

  void _setShutdownState() {
    _setState(ConnectionState.shutdown);
    _cancelTimer();
    _pendingCalls.forEach(_shutdownCall);
    _pendingCalls.clear();
  }

  void _setState(ConnectionState state) {
    _state = state;
    onStateChanged?.call(state);
  }

  void _handleIdleTimeout() {
    if (_timer == null || _state != ConnectionState.ready) return;
    _cancelTimer();
    _transportConnection?.finish().catchError(
      (_) {},
    ); // TODO(jakobr): Log error.
    keepAliveManager?.onTransportTermination();
    _disconnect();
    _setState(ConnectionState.idle);
  }

  void _cancelTimer() {
    _timer?.cancel();
    _timer = null;
  }

  void _handleActiveStateChanged(bool isActive) {
    if (isActive) {
      _cancelTimer();
      keepAliveManager?.onTransportActive();
    } else {
      if (options.idleTimeout != null) {
        _timer ??= Timer(options.idleTimeout!, _handleIdleTimeout);
      }
      keepAliveManager?.onTransportIdle();
    }
  }

  bool _hasPendingCalls() {
    // Get rid of pending calls that have timed out.
    _pendingCalls.removeWhere((call) => call.isCancelled);
    return _pendingCalls.isNotEmpty;
  }

  void _handleConnectionFailure(Object error) {
    _disconnect();
    if (_state == ConnectionState.shutdown || _state == ConnectionState.idle) {
      return;
    }
    // TODO(jakobr): Log error.
    _cancelTimer();
    for (var call in _pendingCalls) {
      _failCall(call, error);
    }
    _pendingCalls.clear();
    keepAliveManager?.onTransportIdle();
    _setState(ConnectionState.idle);
  }

  void _handleReconnect() {
    if (_timer == null || _state != ConnectionState.transientFailure) return;
    _cancelTimer();
    _connect();
  }

  void _disconnect() {
    _transportConnection = null;
    keepAliveManager?.onTransportTermination();
    keepAliveManager = null;
  }

  void _abandonConnection() {
    _cancelTimer();
    _disconnect();

    if (_state == ConnectionState.idle || _state == ConnectionState.shutdown) {
      // All good.
      return;
    }

    // We were not planning to close the socket.
    if (!_hasPendingCalls()) {
      // No pending calls. Just hop to idle, and wait for a new RPC.
      keepAliveManager?.onTransportIdle();
      _setState(ConnectionState.idle);
      return;
    }

    // We have pending RPCs. Reconnect after backoff delay.
    _setState(ConnectionState.transientFailure);
    _currentReconnectDelay = options.backoffStrategy(_currentReconnectDelay);
    _timer = Timer(_currentReconnectDelay!, _handleReconnect);
  }

  static List<Header> createCallHeaders(
    bool useTls,
    String authority,
    String path,
    Duration? timeout,
    Map<String, String>? metadata,
    Codec? compressionCodec, {
    String? userAgent,
    String? grpcAcceptEncodings,
  }) {
    final headers = [
      _methodPost,
      useTls ? _schemeHttps : _schemeHttp,
      Header(ascii.encode(':path'), utf8.encode(path)),
      Header(ascii.encode(':authority'), utf8.encode(authority)),
      if (timeout != null)
        Header.ascii('grpc-timeout', toTimeoutString(timeout)),
      _contentTypeGrpc,
      _teTrailers,
      Header.ascii('user-agent', userAgent ?? defaultUserAgent),
      if (grpcAcceptEncodings != null)
        Header.ascii('grpc-accept-encoding', grpcAcceptEncodings),
      if (compressionCodec != null)
        Header.ascii('grpc-encoding', compressionCodec.encodingName),
    ];
    metadata?.forEach((key, value) {
      headers.add(Header(ascii.encode(key), utf8.encode(value)));
    });
    return headers;
  }
}

/// The maximum size of the response header to a proxy `CONNECT` request.
///
/// Guards against buffering forever when the peer is not an HTTP proxy.
const _maxProxyResponseHeaderBytes = 16 * 1024;

/// `\r\n\r\n`, the end of an HTTP header.
const _headerTerminator = [13, 10, 13, 10];

final _proxyStatusLinePattern = RegExp(r'^HTTP/1\.[01]\s+(\d{3})\b');

class SocketTransportConnector implements ClientTransportConnector {
  /// Either [InternetAddress] or [String].
  final Object _host;
  final int _port;
  final ChannelOptions _options;
  late Socket socket;

  Proxy? get proxy => _options.proxy;
  Object get host => proxy == null ? _host : proxy!.host;
  int get port => proxy == null ? _port : proxy!.port;

  SocketTransportConnector(this._host, this._port, this._options)
    : assert(_host is InternetAddress || _host is String);

  @override
  Future<ClientTransportConnection> connect() async {
    final securityContext = _options.credentials.securityContext;
    var incoming = await connectImpl(proxy);

    // Don't wait for io buffers to fill up before sending requests.
    if (socket.address.type != InternetAddressType.unix) {
      socket.setOption(SocketOption.tcpNoDelay, true);
    }
    if (securityContext != null) {
      socket = await _secure(socket, securityContext);
      incoming = socket;
    }
    return ClientTransportConnection.viaStreams(incoming, socket);
  }

  Future<Stream<List<int>>> connectImpl(Proxy? proxy) async {
    socket = await initSocket(host, port);
    if (proxy == null) {
      return socket;
    }
    return await connectToProxy(proxy);
  }

  Future<Socket> initSocket(Object host, int port) async {
    final proxy = this.proxy;
    try {
      return await Socket.connect(host, port, timeout: _options.connectTimeout);
    } on SocketException catch (error) {
      if (proxy == null) rethrow;
      throw ProxyConnectionException(
        '${_describeRoute(proxy)}: could not connect to the proxy: '
        '${_describeSocketError(error)}',
      );
    }
  }

  Future<SecureSocket> _secure(
    Socket socket,
    SecurityContext securityContext,
  ) async {
    final proxy = this.proxy;
    final handshake = SecureSocket.secure(
      socket,
      // This is not really the host, but the authority to verify the TLC
      // connection against. When tunneling through a proxy, this is the
      // target and not the proxy.
      //
      // We don't use `this.authority` here, as that includes the port.
      host: _options.credentials.authority ?? _host,
      context: securityContext,
      onBadCertificate: _validateBadCertificate,
      // Without ALPN, middleboxes may treat the connection as HTTP/1.1.
      supportedProtocols: const ['h2'],
    );

    final SecureSocket secureSocket;
    try {
      secureSocket = await handshake.timeout(_options.tlsHandshakeTimeout);
    } on TimeoutException {
      // Once handed to `SecureSocket.secure`, destroying `socket` does
      // nothing. Make sure a handshake completing after the timeout does not
      // leave a connection behind.
      unawaited(
        handshake.then((secured) => secured.destroy(), onError: (Object _) {}),
      );
      socket.destroy();
      final message =
          'TLS handshake did not complete within '
          '${_options.tlsHandshakeTimeout.inMilliseconds} ms';
      if (proxy == null) {
        throw TimeoutException(
          '$message: $authority',
          _options.tlsHandshakeTimeout,
        );
      }
      throw ProxyConnectionException('${_describeRoute(proxy)}: $message');
    } catch (error) {
      socket.destroy();
      if (proxy == null) rethrow;
      throw ProxyConnectionException(
        '${_describeRoute(proxy)}: TLS handshake through the tunnel failed: '
        '$error',
      );
    }

    final selectedProtocol = secureSocket.selectedProtocol;
    // `null` means the server does not support ALPN, which is allowed.
    if (selectedProtocol != null && selectedProtocol != 'h2') {
      secureSocket.destroy();
      final message =
          'ALPN negotiated "$selectedProtocol" instead of "h2", '
          'HTTP/2 is not available';
      if (proxy == null) {
        throw TransportException('$authority: $message');
      }
      throw ProxyConnectionException('${_describeRoute(proxy)}: $message');
    }
    return secureSocket;
  }

  void _sendConnect(Map<String, String> headers) {
    const linebreak = '\r\n';
    socket.write('CONNECT $_connectTarget HTTP/1.1');
    socket.write(linebreak);
    headers.forEach((key, value) {
      socket.write('$key: $value');
      socket.write(linebreak);
    });
    socket.write(linebreak);
  }

  /// The `host:port` of the target, as used in the `CONNECT` request.
  String get _connectTarget {
    final host = _host;
    final hostName = host is InternetAddress ? host.address : host as String;
    // IPv6 literals must be enclosed in brackets.
    return hostName.contains(':') ? '[$hostName]:$_port' : '$hostName:$_port';
  }

  @override
  String get authority {
    return _options.credentials.authority ?? _makeAuthority();
  }

  String _makeAuthority() {
    final host = _host;
    final portSuffix = _port == 443 ? '' : ':$_port';
    final String hostName;
    if (host is String) {
      hostName = host;
    } else {
      host as InternetAddress;
      if (host.type == InternetAddressType.unix) {
        return 'localhost'; // UDS don't have a meaningful authority.
      }
      hostName = host.host;
    }
    return '$hostName$portSuffix';
  }

  @override
  Future get done {
    ArgumentError.checkNotNull(socket);
    return socket.done;
  }

  @override
  void shutdown() {
    ArgumentError.checkNotNull(socket);
    socket.destroy();
  }

  bool _validateBadCertificate(X509Certificate certificate) {
    final credentials = _options.credentials;
    final validator = credentials.onBadCertificate;

    if (validator == null) return false;
    return validator(certificate, authority);
  }

  Future<Stream<List<int>>> connectToProxy(Proxy proxy) async {
    final headers = {'Host': _connectTarget};
    if (proxy.isAuthenticated) {
      // If the proxy configuration contains user information use that
      // for proxy basic authorization.
      final authStr = '${proxy.username}:${proxy.password}';
      final auth = base64Encode(utf8.encode(authStr));
      headers[HttpHeaders.proxyAuthorizationHeader] = 'Basic $auth';
    }
    final isSecure = _options.credentials.isSecure;
    final completer = Completer<void>();
    final buffer = <int>[];

    late final StreamSubscription<Uint8List> subscription;

    /// Routes the events through after the tunnel has been established.
    final intermediate = StreamController<List<int>>(
      onPause: () => subscription.pause(),
      onResume: () => subscription.resume(),
      onCancel: () => subscription.cancel(),
    );

    // When using TLS, this subscription must stay active until the socket is
    // handed to `SecureSocket.secure`, which takes it over.
    subscription = socket.listen(
      (chunk) {
        if (completer.isCompleted) {
          intermediate.add(chunk);
          return;
        }
        buffer.addAll(chunk);
        final headerEnd = _indexOfHeaderTerminator(buffer);
        if (headerEnd < 0) {
          if (buffer.length > _maxProxyResponseHeaderBytes) {
            completer.completeError(
              ProxyConnectionException(
                '${_describeRoute(proxy)}: the response to CONNECT did not '
                'end within $_maxProxyResponseHeaderBytes bytes',
              ),
            );
          }
          return;
        }
        final header = latin1.decode(buffer.sublist(0, headerEnd));
        final leftover = buffer.sublist(headerEnd + _headerTerminator.length);
        try {
          _verifyProxyResponse(proxy, header);
          if (leftover.isNotEmpty) {
            if (isSecure) {
              // The client starts the TLS handshake, so the server has
              // nothing to send yet.
              throw ProxyConnectionException(
                '${_describeRoute(proxy)}: received ${leftover.length} '
                'unexpected bytes after the response to CONNECT',
              );
            }
            // Already part of the HTTP/2 connection.
            intermediate.add(Uint8List.fromList(leftover));
          }
          completer.complete();
        } on ProxyConnectionException catch (error) {
          completer.completeError(error);
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        if (completer.isCompleted) {
          intermediate.addError(error, stackTrace);
          return;
        }
        completer.completeError(
          ProxyConnectionException(
            '${_describeRoute(proxy)}: could not read the response to '
            'CONNECT: $error',
          ),
          stackTrace,
        );
      },
      onDone: () {
        if (completer.isCompleted) {
          intermediate.close();
          return;
        }
        completer.completeError(
          ProxyConnectionException(
            '${_describeRoute(proxy)}: the proxy closed the connection '
            'before responding to CONNECT',
          ),
        );
      },
    );

    _sendConnect(headers);

    try {
      await completer.future.timeout(
        _options.proxyConnectResponseTimeout,
        onTimeout: () => throw ProxyConnectionException(
          '${_describeRoute(proxy)}: no response to CONNECT within '
          '${_options.proxyConnectResponseTimeout.inMilliseconds} ms',
        ),
      );
    } catch (_) {
      await subscription.cancel();
      socket.destroy();
      rethrow;
    }
    return intermediate.stream;
  }

  /// Verifies the response header (without the terminating empty line) to the
  /// `CONNECT` request.
  void _verifyProxyResponse(Proxy proxy, String header) {
    final lines = header.split('\r\n');
    final statusLine = lines.first;
    final match = _proxyStatusLinePattern.firstMatch(statusLine);
    if (match == null) {
      throw ProxyConnectionException(
        '${_describeRoute(proxy)}: the response to CONNECT is not HTTP: '
        '"$statusLine"',
      );
    }
    final statusCode = int.parse(match.group(1)!);
    if (statusCode >= 200 && statusCode < 300) return;

    final message = StringBuffer(
      '${_describeRoute(proxy)}: the proxy rejected CONNECT ($statusLine)',
    );
    if (statusCode == HttpStatus.proxyAuthenticationRequired) {
      final schemes = lines
          .where((line) => line.toLowerCase().startsWith('proxy-authenticate:'))
          .map((line) => line.substring(line.indexOf(':') + 1).trim())
          .where((scheme) => scheme.isNotEmpty);
      message.write(
        '. Proxy authentication is required '
        '(${schemes.isEmpty ? 'unknown scheme' : schemes.join(', ')}); '
        'only Basic authentication is supported',
      );
    }
    throw ProxyConnectionException(message.toString());
  }

  String _describeRoute(Proxy proxy) =>
      'Connecting to $_connectTarget via proxy ${proxy.host}:${proxy.port}';

  /// [SocketException.toString] shows the local port, which is misleading
  /// here.
  static String _describeSocketError(SocketException error) {
    final osError = error.osError;
    if (osError == null) return error.message;
    return '${error.message} (${osError.message}, errno = ${osError.errorCode})';
  }

  static int _indexOfHeaderTerminator(List<int> bytes) {
    outer:
    for (var i = 0; i + _headerTerminator.length <= bytes.length; i++) {
      for (var j = 0; j < _headerTerminator.length; j++) {
        if (bytes[i + j] != _headerTerminator[j]) continue outer;
      }
      return i;
    }
    return -1;
  }
}

class _ShutdownException implements Exception {}
