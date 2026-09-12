// L2, Dart side: one duplex link per line over a WebSocket. dart:io's WebSocket is unavailable at
// compile time on web, so the actual implementation lives in link_io.dart (native) or
// link_web.dart (browser, via package:web) and this file just picks one — everything above L2
// (redundant.dart, client.dart) imports this file and never needs to know which platform it's on.
export 'link_io.dart' if (dart.library.js_interop) 'link_web.dart';
