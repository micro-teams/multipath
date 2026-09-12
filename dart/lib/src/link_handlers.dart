// Shared between the dart:io and web L2 link implementations (link_io.dart / link_web.dart):
// callbacks are platform-independent, so they live in their own file rather than being duplicated.

import 'frames.dart';

class LinkHandlers {
  final void Function() onOpen;
  final void Function(Frame) onFrame;
  final void Function() onClose;
  LinkHandlers(
      {required this.onOpen, required this.onFrame, required this.onClose});
}
